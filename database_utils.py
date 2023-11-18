import yaml
from sqlalchemy import create_engine

class DatabaseConnector():
    def __init__(self, filename):
        self.filename = filename
    
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
    

your_instance = DatabaseConnector(filename="db_creds.yaml")
# Initialize the SQLAlchemy engine using the default file name "db_creds.yaml"
db_engine = your_instance.init_db_engine()
