from sqlalchemy import create_engine, inspect
import yaml

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

    #upload db to pgadmin
    def upload_to_db(self, df, table_name):
        sql_engine = create_engine(f"postgresql://localhost:5432/sales_data?user=postgres&password=Pigeon152.")    
        df.to_sql(table_name, sql_engine, if_exists='replace', index=False)