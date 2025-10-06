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
    Carga archivos Parquet de NYC Taxi en Snowflake (schema BRONZE).
    Si el trigger no pasa argumentos, usa valores por defecto.
    """

    # ✅ Leer parámetros dinámicos del trigger o usar valores por defecto
    service_type = kwargs.get('service_type', 'yellow')
    year = int(kwargs.get('year', 2025))

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

    for month in range(1, 7):
    # for month in [10, 11, 12]:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service_type}_tripdata_{year}-{month:02d}.parquet"
        print(f"📦 Procesando {url} ...")

        try:
            response = requests.get(url, timeout=300)
            response.raise_for_status()

            df = pd.read_parquet(io.BytesIO(response.content))
            df.columns = [re.sub(r'[^0-9A-Z_]', '_', c.upper()) for c in df.columns]

            df["LOAD_YEAR"] = year
            df["LOAD_MONTH"] = month
            df["LOAD_SERVICE_TYPE"] = service_type

            write_pandas(
                conn,
                df,
                f"{service_type.upper()}_TRIPS_BRONZE",
                auto_create_table=True,
                overwrite=False
            )

            print(f"✅ {service_type}_{year}-{month:02d} cargado ({len(df)} filas)")

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

    coverage_df = pd.DataFrame(all_coverage)
    coverage_df.columns = [c.upper() for c in coverage_df.columns]
    write_pandas(conn, coverage_df, "COVERAGE_MATRIX", auto_create_table=True, overwrite=False)

    conn.close()
    print(f"🎯 Carga finalizada para {service_type.upper()} {year}")
    return coverage_df
