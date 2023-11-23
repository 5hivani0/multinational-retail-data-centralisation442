from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# Instantiate a DatabaseConnector with the YAML file containing database credentials
db_connector = DatabaseConnector("db_creds.yaml")

# Extract datetime table details, clean and upload
json_s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
date_time_df = data_extractor.json_extract_from_s3(json_s3_address)

cleaning_datetime_data = DataCleaning(date_time_df)
cleaned_datetime_data = cleaning_datetime_data.clean_datetime_data()
print(cleaned_datetime_data)
datetime_table_name = 'dim_date_times'
db_connector.upload_to_db(cleaned_datetime_data, datetime_table_name)
