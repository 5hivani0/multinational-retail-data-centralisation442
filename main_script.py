from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Instantiate a DatabaseConnector with the YAML file containing database credentials
db_connector = DatabaseConnector("db_creds.yaml")

# List tables in the connected database
tables = db_connector.list_db_tables()
print("Tables in the database:", tables)

# Read data from the 'legacy_users' table
user_table_name = 'legacy_users'
data_extractor = DataExtractor()
extracted_data = data_extractor.read_rds_table(user_table_name)
print(f"Data from {user_table_name} as DataFrame:")
print(extracted_data)

cleaning_user_data = DataCleaning(extracted_data)
cleaned_data = cleaning_user_data.clean_user_data()
print(cleaned_data)

table_name = 'dims_users'
db_connector.upload_to_db(cleaned_data, table_name)