import pandas as pd
from sqlalchemy import create_engine, inspect 
from database_utils import DatabaseConnector

class DataExtractor():
    def __init__(self):
        self.db_connector = DatabaseConnector("db_creds.yaml")
        self.engine = self.db_connector.engine

    def read_rds_table(self, table_name):
        # Read data from the specified table and return as a DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.engine)
        return df



