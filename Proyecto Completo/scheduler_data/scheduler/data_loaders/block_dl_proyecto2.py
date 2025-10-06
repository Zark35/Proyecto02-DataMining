import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.data_preparation.shared.secrets import get_secret_value
import math

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    year, month, service_type = 2019, 1, "yellow"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{service_type}_tripdata_{year}-{month:02d}.parquet"

    df = pd.read_parquet(url)
    df['LOAD_YEAR'] = year
    df['LOAD_MONTH'] = month
    df['LOAD_SERVICE_TYPE'] = service_type

    conn = snowflake.connector.connect(
        user=get_secret_value("SNOWFLAKE_USER"),
        password=get_secret_value("SNOWFLAKE_PASSWORD"),
        account=get_secret_value("SNOWFLAKE_ACCOUNT"),
        warehouse=get_secret_value("SNOWFLAKE_WAREHOUSE"),
        database="NYC_TAXI_DM",
        schema="BRONZE",
        role=get_secret_value("SNOWFLAKE_ROLE"),
    )

    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS BRONZE.{service_type.upper()}_TRIPS_BRONZE")

    # ⚡ Subir en chunks de 100k filas
    chunk_size = 100_000
    total_chunks = math.ceil(len(df) / chunk_size)

    for i in range(total_chunks):
        chunk = df.iloc[i*chunk_size:(i+1)*chunk_size]
        print(f"⬆️ Subiendo chunk {i+1}/{total_chunks} con {len(chunk)} filas...")
        write_pandas(conn, chunk, f"{service_type.upper()}_TRIPS_BRONZE", auto_create_table=True)

    conn.close()
    print("✅ Carga completada")
    return df.head(100)  # devolver solo preview
