from mage_ai.io.snowflake import Snowflake
from mage_ai.io.config import ConfigFileLoader
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data' in globals():
    config_path = 'io_config.yaml'
    config_profile = 'default'

    Snowflake.with_config(ConfigFileLoader(config_path, config_profile)).export(
        data['yellow_trips_2020_01'],
        table_name='yellow_trips_bronze',
        schema_name='BRONZE',
        database_name=get_secret_value('SNOWFLAKE_DATABASE'),
        if_exists='replace',   # en pruebas, luego usar append/upsert
    )
