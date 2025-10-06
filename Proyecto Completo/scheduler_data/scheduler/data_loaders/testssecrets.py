from mage_ai.data_preparation.shared.secrets import get_secret_value

print("ACCOUNT:", get_secret_value("SNOWFLAKE_ACCOUNT"))
print("USER:", get_secret_value("SNOWFLAKE_USER"))
print("DATABASE:", get_secret_value("SNOWFLAKE_DATABASE"))
print("WAREHOUSE:", get_secret_value("SNOWFLAKE_WAREHOUSE"))
print("SCHEMA:", get_secret_value("SNOWFLAKE_SCHEMA"))
