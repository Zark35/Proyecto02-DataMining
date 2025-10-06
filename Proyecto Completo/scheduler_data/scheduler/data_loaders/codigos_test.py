from mage_ai.data_preparation.shared.secrets import get_secret_value
print("ACCOUNT:", get_secret_value("SNOWFLAKE_ACCOUNT"))
print("USER:", get_secret_value("SNOWFLAKE_USER"))
print("ROLE:", get_secret_value("SNOWFLAKE_ROLE"))
