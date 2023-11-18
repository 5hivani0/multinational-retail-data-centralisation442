import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect

class DatabaseConnector():
    def __init__(self, filename):
        self.filename = filename
        self.engine = self.init_db_engine()
    
    """
    Read the database credentials from the YAML file and return a dictionary.
    """
    def read_db_creds(self, filename):
        with open(filename, 'r') as yaml_file:
            db_creds_dict = yaml.safe_load(yaml_file)
        return db_creds_dict

    """
    Read the credentials from the specified YAML file and create an SQLAlchemy database engine.
    """
    def init_db_engine(self):
        creds = self.read_db_creds(self.filename)
        # Construct the database URL for SQLAlchemy
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'

        # Create and return the SQLAlchemy engine
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self):
        # Retrieve and return a list of table names in the connected database
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def read_rds_table(self, table_name):
        # Read data from the specified table and return as a DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.engine)
        return df
    
    def clean_user_data(self, user_data):
        # drop rows with null values
        cleaned_data = user_data.dropna()
        return cleaned_data

# Instantiate a DatabaseConnector with the YAML file containing database credentials
db_connector = DatabaseConnector("db_creds.yaml")

# List tables in the connected database
tables = db_connector.list_db_tables()
print("Tables in the database:", tables)

# Read data from the 'legacy_users' table
table_name = 'legacy_users'
extracted_data = db_connector.read_rds_table(table_name)
print(f"Data from {table_name} as DataFrame:")
print(extracted_data)

# Clean the user data using the clean_user_data method
cleaned_user_data = db_connector.clean_user_data(extracted_data)
print(cleaned_user_data)
