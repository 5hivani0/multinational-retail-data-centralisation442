import pandas as pd
from database_utils import DatabaseConnector
import tabula
import requests
import boto3
from data_cleaning import DataCleaning

class DataExtractor():
    def __init__(self):
        self.db_connector = DatabaseConnector("db_creds.yaml")
        self.engine = self.db_connector.engine
        self.headers = {'x-api-key': api_key}
        self.s3 = boto3.client('s3')

    def read_rds_table(self, table_name):
        # Read data from the specified table and return as a DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.engine)
        return df
    
    def retrieve_pdf_data(self, link_to_pdf):
        pdf_tables = tabula.read_pdf(link_to_pdf, pages='all', multiple_tables=True)
        pdf_df = pd.concat(pdf_tables, ignore_index=True)
        return pdf_df
    
    def list_number_of_stores(self, number_stores_endpoint):
        response = requests.get(number_stores_endpoint, headers=self.headers)
        return response.json()['number_stores']
    
    def retrieve_stores_data(self, store_endpoint_template, num_stores):
        response_data = []

        for store in range(num_stores):
            endpoint = store_endpoint_template + f'/store_details/{store}'
            response = requests.get(endpoint, headers=self.headers)
            response_data.append(response.json())

        store_details = pd.DataFrame(response_data)
        return store_details
    
    def extract_from_s3(self, s3_address):
        parts = s3_address.replace('s3://', '').split('/')
        bucket = parts[0]
        key = '/'.join(parts[1:])
        response = self.s3.get_object(Bucket=bucket, Key=key)
        product_df = pd.read_csv(response['Body'])
        return product_df
    

api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'                                                        

s3_address = "s3://data-handling-public/products.csv"

data_extractor = DataExtractor()
product_df = data_extractor.extract_from_s3(s3_address)

cleaning_product_data = DataCleaning(product_df)
cleaned_product_data = cleaning_product_data.clean_product_data()
print(cleaned_product_data)