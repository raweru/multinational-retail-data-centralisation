import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect


# The `DatabaseConnector` class contains methods for initializing a database engine and uploading data
# from a pandas dataframe to a SQL database table.
class DatabaseConnector:
    def __init__(self):
        
        '''This function initializes database connection parameters from a YAML configuration file.

        '''
        
        config_file = 'ignore_these/sales_data.yaml'
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)

        self.DATABASE_TYPE = config['DATABASE_TYPE']
        self.DBAPI = config['DBAPI']
        self.HOST = config['HOST']
        self.USER = config['USER']
        self.PASSWORD = config['PASSWORD']
        self.DATABASE = config['DATABASE']
        self.PORT = config['PORT']


    def init_db_engine(self):
        
        '''This function initializes a database engine using the specified database type, DBAPI, user,
        password, host, port, and database name.
        
        Returns
        -------
            The function `init_db_engine` returns a SQLAlchemy engine object that connects to a database
        using the parameters specified in the class attributes.
        '''
        
        engine = create_engine(
            f"{self.DATABASE_TYPE}+{self.DBAPI}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}")
        
        return engine


    def upload_to_db(self, dataframe, table_name):
        
        '''This function uploads a pandas dataframe to a SQL database table using the specified engine and
        prints a success message.
        
        Parameters
        ----------
        dataframe
            A pandas DataFrame containing the data to be uploaded to the database.
        table_name
            The name of the table in the database where the data will be uploaded.
        engine
            The engine parameter is an instance of a database connection object that is used to connect to
        a specific database. It is typically created using a database driver and contains information
        such as the database name, host, port, username, and password. The engine is used to execute SQL
        commands and interact with the database
        '''
        
        sales_data_engine = self.init_db_engine()
        dataframe.to_sql(table_name, sales_data_engine, if_exists="replace", index=False)
        
        print(f"Successfully uploaded {table_name} to database!")
    
    
    def list_db_tables(self):
        
        '''This function retrieves and prints the names of all tables in a database using SQLAlchemy's
        inspector.
        '''
        
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        
        print(table_names)