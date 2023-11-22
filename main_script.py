from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Instantiate a DatabaseConnector with the YAML file containing database credentials
db_connector = DatabaseConnector("db_creds.yaml")

# List tables in the connected database
tables = db_connector.list_db_tables()

# Read data from the 'legacy_users' table
user_table_name = 'legacy_users'
data_extractor = DataExtractor()
extracted_data = data_extractor.read_rds_table(user_table_name)



# Extract product details, clean and upload
s3_address = "s3://data-handling-public/products.csv"
product_df = data_extractor.extract_from_s3(s3_address)

cleaning_product_data = DataCleaning(product_df)
cleaned_product_data = cleaning_product_data.convert_product_weights()
print(cleaned_product_data)
product_table_name = 'dim_product'
db_connector.upload_to_db(cleaned_product_data, product_table_name)