import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.data_preparation.shared.secrets import get_secret_value
import re

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data(*args, **kwargs):
    # Parámetros de ingesta (puedes parametrizar con kwargs si quieres más adelante)
    year, month, service_type = 2019, 1, "yellow"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service_type}_tripdata_{year}-{month:02d}.parquet"

    # Leer parquet desde la URL
    df = pd.read_parquet(url)

    # Forzar conversión de columnas datetime
    for col in df.columns:
        if "DATETIME" in col.upper() or "DATE" in col.upper():
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

    # Agregar columnas de control
    df['LOAD_YEAR'] = year
    df['LOAD_MONTH'] = month
    df['LOAD_SERVICE_TYPE'] = service_type

    # Normalizar nombres de columnas
    df.columns = [re.sub(r'[^0-9A-Z_]', '_', c.upper()) for c in df.columns]

    # Conectar a Snowflake
    conn = snowflake.connector.connect(
        user=get_secret_value("SNOWFLAKE_USER"),
        password=get_secret_value("SNOWFLAKE_PASSWORD"),
        account=get_secret_value("SNOWFLAKE_ACCOUNT"),
        warehouse=get_secret_value("SNOWFLAKE_WAREHOUSE"),
        database="NYC_TAXI_DM",
        schema="BRONZE",
        role=get_secret_value("SNOWFLAKE_ROLE"),
    )

    # Variable para definir si es primera carga o no
    first_load = False   # Esta valor es True si es la primera vez para crear la tabla

    # Subir datos de viajes
    write_pandas(
        conn,
        df,
        f"{service_type.upper()}_TRIPS_BRONZE",
        auto_create_table=True,
        overwrite=first_load
    )

    # Crear coverage matrix
    coverage = pd.DataFrame([{
        "LOAD_SERVICE_TYPE": service_type,
        "LOAD_YEAR": year,
        "LOAD_MONTH": month,
        "FILE_EXISTS": True,
        "LOAD_STATUS": "OK"
    }])
    coverage.columns = [c.upper() for c in coverage.columns]

    write_pandas(
        conn,
        coverage,
        "COVERAGE_MATRIX",
        auto_create_table=True,
        overwrite=first_load
    )

    conn.close()
    return df
