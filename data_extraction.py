import boto3
from database_utils import DatabaseConnector
import pandas as pd
import requests
import tabula

api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'

class DataExtractor():
    """
    Class for extracting data from various sources.
    
    Attributes:
        db_connector (DatabaseConnector): Instance of DatabaseConnector for database interaction.
        engine (sqlalchemy.engine.Engine): Database engine for SQL queries.
        headers (dict): HTTP headers for API requests.
        s3 (boto3.client): Amazon S3 client for interacting with S3 storage.
    """
    def __init__(self):
        """
        Initializes a new instance of the DataExtractor class.
        """
        self.db_connector = DatabaseConnector("db_creds.yaml")
        self.engine = self.db_connector.engine
        self.headers = {'x-api-key': api_key}
        self.s3 = boto3.client('s3')

    def read_rds_table(self, table_name):
        """
        Read data from the specified RDS table and return as a DataFrame.

        Args:
            table_name (str): Name of the RDS table.

        Returns:
            pd.DataFrame: DataFrame containing the data from the specified table.
        """
        # Read data from the specified table and return as a DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.engine)
        return df
    
    def retrieve_pdf_data(self, link_to_pdf):
        """
        Retrieve tabular data from a PDF file and return as a DataFrame.

        Args:
            link_to_pdf (str): URL or local path to the PDF file.

        Returns:
            pd.DataFrame: DataFrame containing the extracted data from the PDF.
        """
        pdf_tables = tabula.read_pdf(link_to_pdf, pages='all', multiple_tables=True)
        pdf_df = pd.concat(pdf_tables, ignore_index=True)
        return pdf_df
    
    def list_number_of_stores(self, number_stores_endpoint):
        """
        Retrieve the number of stores from an API endpoint.

        Args:
            number_stores_endpoint (str): API endpoint to get the number of stores.

        Returns:
            int: Number of stores.
        """
        response = requests.get(number_stores_endpoint, headers=self.headers)
        return response.json()['number_stores']
    
    def retrieve_stores_data(self, store_endpoint_template, num_stores):
        """
        Retrieve store details for the specified number of stores from an API endpoint.

        Args:
            store_endpoint_template (str): API endpoint template for store details.
            num_stores (int): Number of stores to retrieve details for.

        Returns:
            pd.DataFrame: DataFrame containing store details.
        """
        response_data = []

        for store in range(num_stores):
            endpoint = store_endpoint_template + f'/store_details/{store}'
            response = requests.get(endpoint, headers=self.headers)
            response_data.append(response.json())

        store_details = pd.DataFrame(response_data)
        return store_details
    
    def csv_extract_from_s3(self, s3_address):
        """
        Extract CSV data from an S3 address and return as a DataFrame.

        Args:
            s3_address (str): S3 address in the format 's3://bucket/key'.

        Returns:
            pd.DataFrame: DataFrame containing the CSV data.
        """
        parts = s3_address.replace('s3://', '').split('/')
        bucket = parts[0]
        key = '/'.join(parts[1:])
        response = self.s3.get_object(Bucket=bucket, Key=key)
        product_df = pd.read_csv(response['Body'])
        return product_df
    
    def json_extract_from_s3(self, s3_address):
        """
        Extract JSON data from an S3 address and return as a DataFrame.

        Args:
            s3_address (str): S3 address to the JSON file.

        Returns:
            pd.DataFrame: DataFrame containing the JSON data.
        """
        s3_url = s3_address
        datetime_df = pd.read_json(s3_url)
        return datetime_df