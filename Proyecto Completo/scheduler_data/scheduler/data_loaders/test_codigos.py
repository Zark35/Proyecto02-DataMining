import snowflake.connector
from mage_ai.data_preparation.shared.secrets import get_secret_value
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data(*args, **kwargs):
    conn = snowflake.connector.connect(
        user=get_secret_value("SNOWFLAKE_USER"),
        password=get_secret_value("SNOWFLAKE_PASSWORD"),
        account=get_secret_value("SNOWFLAKE_ACCOUNT"),
        warehouse=get_secret_value("SNOWFLAKE_WAREHOUSE"),
        database="NYC_TAXI_DM",
        schema="BRONZE",
        role=get_secret_value("SNOWFLAKE_ROLE"),
    )
    print("✅ conectado")
    conn.close()
