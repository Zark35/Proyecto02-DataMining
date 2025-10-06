import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.data_preparation.shared.secrets import get_secret_value
import re

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data(*args, **kwargs):
    """
    Carga los datos del servicio GREEN TAXI (enero 2019) en Snowflake -> esquema BRONZE.
    Limpia columnas conflictivas y añade metadatos después de la carga.
    """

    year, month, service_type = 2019, 1, "green"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service_type}_tripdata_{year}-{month:02d}.parquet"

    print(f"📦 Cargando datos desde: {url}")
    df = pd.read_parquet(url)

    # Normalizar nombres de columnas
    df.columns = [re.sub(r'[^0-9A-Z_]', '_', c.upper()) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    df = df[[c for c in df.columns if c.strip() != '']]

    # Quitar columnas problemáticas
    invalid_cols = [
        'EHAIL_FEE', 'TRIP_TYPE', 'ACCESS_A_RIDE_FLAG',
        'SHARED_REQUEST_FLAG', 'SHARED_MATCH_FLAG'
    ]
    for col in invalid_cols:
        if col in df.columns:
            print(f"⚠️  Eliminando columna no soportada: {col}")
            df.drop(columns=[col], inplace=True)

    # Convertir fechas
    for col in df.columns:
        if "DATETIME" in col.upper() or "DATE" in col.upper():
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    print(f"✅ Registros listos para subir: {len(df)}")

    # Conexión a Snowflake
    conn = snowflake.connector.connect(
        user=get_secret_value("SNOWFLAKE_USER"),
        password=get_secret_value("SNOWFLAKE_PASSWORD"),
        account=get_secret_value("SNOWFLAKE_ACCOUNT"),
        warehouse=get_secret_value("SNOWFLAKE_WAREHOUSE"),
        database="NYC_TAXI_DM",
        schema="BRONZE",
        role=get_secret_value("SNOWFLAKE_ROLE"),
    )

    table_name = f"{service_type.upper()}_TRIPS_BRONZE"

    print("🚀 Subiendo datos a Snowflake (sin columnas de control)...")

    write_pandas(
        conn,
        df,
        table_name,
        auto_create_table=True,
        overwrite=True
    )

    print("✅ Datos subidos correctamente.")

    # Añadir columnas de control directamente en Snowflake
    with conn.cursor() as cur:
        print("🧩 Verificando columnas de control en Snowflake...")

        # ✅ Quitar "IF NOT EXISTS" después de ALTER TABLE
        cur.execute(f"""
            ALTER TABLE {table_name}
            ADD IF NOT EXISTS LOAD_YEAR INT,
                            LOAD_MONTH INT,
                            LOAD_SERVICE_TYPE STRING;
        """)

        cur.execute(f"""
            UPDATE {table_name}
            SET LOAD_YEAR = {year},
                LOAD_MONTH = {month},
                LOAD_SERVICE_TYPE = '{service_type}'
            WHERE LOAD_YEAR IS NULL;
        """)

    # Registrar en coverage matrix
    coverage = pd.DataFrame([{
        "LOAD_SERVICE_TYPE": service_type,
        "LOAD_YEAR": year,
        "LOAD_MONTH": month,
        "FILE_EXISTS": True,
        "LOAD_STATUS": "OK"
    }])
    coverage.columns = [c.upper() for c in coverage.columns]
    write_pandas(conn, coverage, "COVERAGE_MATRIX", auto_create_table=True, overwrite=False)

    print("📊 Coverage Matrix actualizada.")
    conn.close()
    print("🔒 Conexión cerrada correctamente.")
    return df
