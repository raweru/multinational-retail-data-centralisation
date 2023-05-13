import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DataExtractor:
    def __init__(self):
        self.engine = None
    
    def read_db_creds(self, filename: str) -> dict:
        '''This function reads a YAML file containing database credentials and returns them as a dictionary.
        
        Parameters
        ----------
        filename : str
            The filename parameter is a string that represents the name of the file that contains the database
        credentials.
        
        Returns
        -------
            A dictionary containing the database credentials read from the specified file.
        
        '''
        with open(filename, 'r') as f:
            creds = yaml.safe_load(f)
            
        return creds
    
    def init_db_engine(self, creds: dict):
        '''This function initializes a PostgreSQL database engine using the provided credentials.
        
        Parameters
        ----------
        creds : dict
            The `creds` parameter is a dictionary that contains the credentials needed to connect to a
        PostgreSQL database. It should have the following keys:
        
        Returns
        -------
            The function `init_db_engine` returns a SQLAlchemy engine object that connects to a PostgreSQL
        database using the credentials provided in the `creds` dictionary.
        
        '''
        url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        self.engine = create_engine(url)
        
        return self.engine
    
    def list_db_tables(self):
        '''This function inspects a database engine and prints out a list of table names.
        
        '''
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        print(table_names)
    
    def read_rds_table(self, table_name: str) -> pd.DataFrame:
        '''This function reads a table from a database using the specified table name and returns it as a
        pandas DataFrame.
        
        Parameters
        ----------
        table_name : str
            The name of the table in the database that you want to read.
        
        Returns
        -------
            The function `read_rds_table` returns a pandas DataFrame containing the data from the specified
        table in the database.
        
        '''
        with self.engine.connect() as conn:
            df = pd.read_sql_table(table_name, con=conn)
            
        return df

