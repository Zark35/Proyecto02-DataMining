import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.data_preparation.shared.secrets import get_secret_value
import re
import requests
import io

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data(**kwargs):
    """
    Carga archivos Parquet del servicio GREEN TAXI en Snowflake (schema BRONZE).
    Descarga todos los meses del año indicado y registra cobertura.
    """

    # ✅ Parámetros dinámicos o valores por defecto
    service_type = kwargs.get('service_type', 'green')
    year = int(kwargs.get('year', 2018))

    print(f"🚀 Iniciando carga para {service_type.upper()} {year}...")

    # 🔐 Credenciales Snowflake desde Mage Secrets
    conn = snowflake.connector.connect(
        user=get_secret_value("SNOWFLAKE_USER"),
        password=get_secret_value("SNOWFLAKE_PASSWORD"),
        account=get_secret_value("SNOWFLAKE_ACCOUNT"),
        warehouse=get_secret_value("SNOWFLAKE_WAREHOUSE"),
        database="NYC_TAXI_DM",
        schema="BRONZE",
        role=get_secret_value("SNOWFLAKE_ROLE"),
    )

    all_coverage = []
    table_name = f"{service_type.upper()}_TRIPS_BRONZE"

    # 🔁 Recorrer los meses del año
    for month in range(1, 13):
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service_type}_tripdata_{year}-{month:02d}.parquet"
        print(f"\n📦 Procesando {url} ...")

        try:
            # Descargar Parquet
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            # Leer en DataFrame
            df = pd.read_parquet(io.BytesIO(response.content))

            # Normalizar nombres de columnas
            df.columns = [re.sub(r'[^0-9A-Z_]', '_', c.upper()) for c in df.columns]
            df = df.loc[:, ~df.columns.duplicated()]
            df = df[[c for c in df.columns if c.strip() != '']]

            # Eliminar columnas problemáticas si existen
            invalid_cols = [
                'EHAIL_FEE', 'TRIP_TYPE', 'ACCESS_A_RIDE_FLAG',
                'SHARED_REQUEST_FLAG', 'SHARED_MATCH_FLAG'
            ]
            for col in invalid_cols:
                if col in df.columns:
                    print(f"⚠️  Eliminando columna no soportada: {col}")
                    df.drop(columns=[col], inplace=True)

            # Convertir columnas de fecha/hora si es posible
            for col in df.columns:
                if "DATETIME" in col.upper() or "DATE" in col.upper():
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except Exception:
                        pass

            # Agregar metadatos
            df["LOAD_YEAR"] = year
            df["LOAD_MONTH"] = month
            df["LOAD_SERVICE_TYPE"] = service_type

            # Cargar en Snowflake
            write_pandas(
                conn,
                df,
                table_name,
                auto_create_table=True,
                overwrite=False  # idempotencia
            )

            print(f"✅ {service_type}_{year}-{month:02d} cargado ({len(df)} filas)")

            # Registrar éxito
            all_coverage.append({
                "LOAD_SERVICE_TYPE": service_type,
                "LOAD_YEAR": year,
                "LOAD_MONTH": month,
                "FILE_EXISTS": True,
                "LOAD_STATUS": "OK"
            })

        except Exception as e:
            print(f"⚠️ Error con {url}: {e}")
            all_coverage.append({
                "LOAD_SERVICE_TYPE": service_type,
                "LOAD_YEAR": year,
                "LOAD_MONTH": month,
                "FILE_EXISTS": False,
                "LOAD_STATUS": "ERROR"
            })

    # 🧾 Registrar matriz de cobertura
    coverage_df = pd.DataFrame(all_coverage)
    coverage_df.columns = [c.upper() for c in coverage_df.columns]
    write_pandas(conn, coverage_df, "COVERAGE_MATRIX", auto_create_table=True, overwrite=False)

    conn.close()
    print(f"\n🎯 Carga finalizada para {service_type.upper()} {year}")
    return coverage_df
