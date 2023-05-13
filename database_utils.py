import pandas as pd
import numpy as np
import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import tabula

# The class defines the parameters for connecting to a PostgreSQL database using psycopg2.
class DatabaseConnector:
    def __init__(self):
        self.DATABASE_TYPE = 'postgresql'
        self.DBAPI = 'psycopg2'
        self.HOST = 'localhost'
        self.USER = 'postgres'
        self.PASSWORD = 'J3j27G.XxlF53'
        self.DATABASE = 'sales_data'
        self.PORT = 5432

    def init_db_engine(self):
        '''This function initializes a database engine using the specified database type, DBAPI, user
        credentials, host, port, and database name.
        
        Returns
        -------
            The function `init_db_engine` returns a SQLAlchemy engine object that connects to a database
        using the parameters specified in the class attributes.
        
        '''
        engine = create_engine(f"{self.DATABASE_TYPE}+{self.DBAPI}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}")
    
        return engine
    
    def upload_to_db(self, dataframe, table_name, engine):
        '''This function uploads a pandas dataframe to a SQL database table using the specified engine and
        replaces the table if it already exists.
        
        Parameters
        ----------
        dataframe
            A pandas DataFrame containing the data to be uploaded to the database.
        table_name
            The name of the table in the database where the data from the dataframe will be uploaded.
        engine
            The engine parameter is an instance of a database connection created using a database API such
        as SQLAlchemy. It is used to connect to a database and execute SQL commands.
        
        '''
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)



if __name__ == '__main__':
    def upload_user_data_to_db():
        database_extractor = DataExtractor()
        # read credentials from yaml file
        creds_dict = database_extractor.read_db_creds("db_creds.yaml")
        # create engine 1
        db_engine = database_extractor.init_db_engine(creds_dict)
        # read user table
        user_table = database_extractor.read_rds_table("legacy_users")
        
        data_cleaner = DataCleaning()
        # clean user table
        user_table = data_cleaner.clean_user_data(user_table)
        
        data_connector = DatabaseConnector()
        # connect to sales_data database
        sales_data_engine = data_connector.init_db_engine()
        # upload user_table to sales_data
        data_connector.upload_to_db(user_table, "dim_users", sales_data_engine)
    
    def upload_card_data_to_db():
        database_extractor = DataExtractor()
        # read card pdf to 
        card_table = database_extractor.retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        
        data_cleaner = DataCleaning()
        # clean card table
        user_table = data_cleaner.clean_card_data(card_table)
        
        data_connector = DatabaseConnector()
        # connect to sales_data database
        sales_data_engine = data_connector.init_db_engine()
        # upload card_table to sales_data
        data_connector.upload_to_db(user_table, "dim_card_details", sales_data_engine)

    upload_card_data_to_db()