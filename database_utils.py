import yaml
from sqlalchemy import create_engine
import pandas as pd

class DatabaseConnector:
    def __init__(self):
        pass
    
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
        engine = create_engine(url)
        
        return engine
    
    def list_db_tables(self, engine):
        '''This function returns a list of table names in a database engine.
        
        Parameters
        ----------
        engine
            The engine parameter is an instance of a database engine, which is used to connect to a database
        and execute SQL queries. It is typically created using a library such as SQLAlchemy. The engine
        object contains information about the database connection, such as the database URL, username, and
        password.
        
        Returns
        -------
            a list of table names in the database connected to the provided engine.
        
        '''
        tables = engine.table_names()
        
        return tables
    
    def read_rds_table(self, engine, table_name):
        '''This function reads a table from a database engine and returns it as a pandas dataframe.
        
        Parameters
        ----------
        engine
            The engine parameter is a SQLAlchemy engine object that is used to connect to a database. It
        contains information about the database connection, such as the database type, host, port, username,
        and password. The engine object is used to execute SQL queries and retrieve data from the database.
        table_name
            The name of the table in the database that you want to read.
        
        Returns
        -------
            a pandas DataFrame that contains the data from the specified table in the specified database
        engine.
        
        '''
        df = pd.read_sql_table(table_name, engine)
        
        return df

if __name__ == '__main__':
    database_connector = DatabaseConnector()
    creds_dict = database_connector.read_db_creds("db_creds.yaml")
    creds_dict
    db_engine = database_connector.init_db_engine(creds_dict)
    db_engine
    database_connector.list_db_tables(db_engine)
