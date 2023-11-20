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

# Clean legacy_user data
cleaning_user_data = DataCleaning(extracted_data)
cleaned_data = cleaning_user_data.clean_user_data()
print(cleaned_data)

# Upload cleaned legacy_user data to pgadmin sales data
table_name = 'dims_users'
db_connector.upload_to_db(cleaned_data, table_name)

# Extract pdf table data
link_to_pdf = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
pdf_data = data_extractor.retrieve_pdf_data(link_to_pdf)
print(pdf_data)

cleaning_pdf_data = DataCleaning(pdf_data)
cleaned_pdf_data = cleaning_pdf_data.clean_card_data()
print(cleaned_pdf_data)

pdf_table_name = 'dim_card_details'
db_connector.upload_to_db(cleaned_pdf_data, pdf_table_name)
