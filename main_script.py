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
user_extracted_data = data_extractor.read_rds_table(user_table_name)

# Clean legacy_user data
cleaning_user_data = DataCleaning(user_extracted_data)
user_cleaned_data = cleaning_user_data.clean_user_data()

# Upload cleaned legacy_user data to pgadmin sales data
table_name = 'dims_users'
db_connector.upload_to_db(user_cleaned_data, table_name)

# Extract pdf table data
link_to_pdf = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
pdf_data = data_extractor.retrieve_pdf_data(link_to_pdf)

cleaning_pdf_data = DataCleaning(pdf_data)
cleaned_pdf_data = cleaning_pdf_data.clean_card_data()

pdf_table_name = 'dim_card_details'
db_connector.upload_to_db(cleaned_pdf_data, pdf_table_name)

# Extract store details, clean and upload
number_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
store_endpoint_template = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod'

num_stores = data_extractor.list_number_of_stores(number_stores_endpoint)
stores_data_df = data_extractor.retrieve_stores_data(store_endpoint_template, num_stores)

cleaning_store_data = DataCleaning(stores_data_df)
cleaned_store_data = cleaning_store_data.clean_store_data()

store_table_name = 'dim_store_details'
db_connector.upload_to_db(cleaned_store_data, store_table_name)

# Extract product details, clean and upload
s3_address = "s3://data-handling-public/products.csv"
product_df = data_extractor.extract_from_s3(s3_address)

cleaning_product_data = DataCleaning(product_df)
cleaned_product_data = cleaning_product_data.clean_product_data()
product_table_name = 'dim_product'
db_connector.upload_to_db(cleaned_product_data, product_table_name)

# Extract orders table details, clean and upload
orders_table_name = 'orders_table'
orders_extracted_data = data_extractor.read_rds_table(orders_table_name)

cleaning_orders_data = DataCleaning(orders_extracted_data)
order_cleaned_data = cleaning_orders_data.clean_orders_data()