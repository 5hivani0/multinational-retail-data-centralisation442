import pandas as pd
from sqlalchemy import create_engine, inspect 
from database_utils import DatabaseConnector
import tabula
from data_cleaning import DataCleaning

class DataExtractor():
    def __init__(self):
        self.db_connector = DatabaseConnector("db_creds.yaml")
        self.engine = self.db_connector.engine

    def read_rds_table(self, table_name):
        # Read data from the specified table and return as a DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.engine)
        return df
    
    def retrieve_pdf_data(self, link_to_pdf):
        pdf_tables = tabula.read_pdf(link_to_pdf, pages='all', multiple_tables=True)
        pdf_df = pd.concat(pdf_tables, ignore_index=True)
        return pdf_df

data_extractor = DataExtractor()
link_to_pdf = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
pdf_data = data_extractor.retrieve_pdf_data(link_to_pdf)

cleaning_pdf_data = DataCleaning(pdf_data)
cleaned_pdf_data = cleaning_pdf_data.clean_card_data()
print(cleaned_pdf_data)