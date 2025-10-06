import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.data_preparation.shared.secrets import get_secret_value
import re

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


@data_loader
def load_data(*args, **kwargs):
    # URL del lookup oficial de zonas
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

    # Leer CSV
    df = pd.read_csv(url)

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

    # 🚦 Switch para definir si es primera carga o no
    first_load = False   # ponlo en True la primera vez

    # Subir datos a tabla Bronze
    write_pandas(
        conn,
        df,
        "TAXI_ZONES_BRONZE",
        auto_create_table=True,
        overwrite=first_load
    )

    conn.close()
    return df
