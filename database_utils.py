from sqlalchemy import create_engine, inspect
import yaml

class DatabaseConnector():
    """
    A utility class for connecting to a PostgreSQL database and performing database operations.

    Attributes:
        filename: Name of the YAML file containing database credentials.
        engine: SQLAlchemy engine for database interaction.
    """
    def __init__(self, filename):
        """
        Initializes a new instance of the DatabaseConnector class.

        Args:
            filename: Name of the YAML file containing database credentials.
        """
        self.filename = filename
        self.engine = self.init_db_engine()
    
    def read_db_creds(self, filename):
        """
        Read the database credentials from the specified YAML file and return a dictionary.

        Args:
            filename: Name of the YAML file.

        Returns:
            dict: Dictionary containing database credentials.
        """
        with open(filename, 'r') as yaml_file:
            db_creds_dict = yaml.safe_load(yaml_file)
        return db_creds_dict
    
    def init_db_engine(self):
        """
        Initialize and return a SQLAlchemy engine using the credentials from the YAML file.

        Returns:
            engine: SQLAlchemy engine for connecting to the database.
        """
        creds = self.read_db_creds(self.filename)
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine
    
    def list_db_tables(self):
        """
        List all tables in the connected PostgreSQL database.

        Returns:
            list: List of table names in the connected database.
        """
        inspector = inspect(self.engine)
        return inspector.get_table_names()

    def upload_to_db(self, df, table_name):
        """
        Upload a DataFrame to a specified table in the connected PostgreSQL database.

        Args:
            df: DataFrame to be uploaded.
            table_name: Name of the target table in the database.
        """
        db_details = {
            'host': '', # enter host here
            'port': '', # enter port here
            'database': '', # enter your database name here
            'user' : '', # enter user here
            'password': '', # enter your password here  
        }
        sql_engine = create_engine(f"postgresql://{db_details['host']}:{db_details['port']}/{db_details['database']}?user={db_details['user']}&password={db_details['password']}")
        df.to_sql(table_name, sql_engine, if_exists='replace', index=False)