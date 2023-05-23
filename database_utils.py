from sqlalchemy import create_engine
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


# The `DatabaseConnector` class contains methods for initializing a database engine and uploading data
# from a pandas dataframe to a SQL database table.
class DatabaseConnector:
    def __init__(self):
        """
        This is a constructor function that initializes database connection parameters for a PostgreSQL
        database.
        """
        self.DATABASE_TYPE = "postgresql"
        self.DBAPI = "psycopg2"
        self.HOST = "localhost"
        self.USER = "postgres"
        self.PASSWORD = "password"
        self.DATABASE = "sales_data"
        self.PORT = 5432

    def init_db_engine(self):
        """
        This function initializes a database engine using the specified database type, DBAPI, user
        credentials, host, port, and database name.

        Returns:
        The function `init_db_engine` returns a SQLAlchemy engine object that connects to a database
        using the parameters specified in the class attributes.
        """
        engine = create_engine(
            f"{self.DATABASE_TYPE}+{self.DBAPI}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}"
        )

        return engine

    def upload_to_db(self, dataframe, table_name, engine):
        """
        This function uploads a pandas dataframe to a SQL database table using the specified engine and
        table name, replacing any existing data in the table.

        Args:
            dataframe: A pandas DataFrame containing the data to be uploaded to the database.
            table_name: The name of the table in the database where the data from the dataframe will be
        uploaded.
            engine: The engine parameter is an instance of a database connection object created using a
        database API such as SQLAlchemy. It is used to connect to a database and execute SQL commands.
        In this case, it is used to connect to a database and upload a pandas dataframe to a specified
        table in the database.
        """
        dataframe.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Successfully uploaded {table_name} to database!")


if __name__ == "__main__":

    def upload_user_data_to_db():
        """
        This function uploads cleaned user data from a legacy database to sales_data database.
        """
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
        """
        This function uploads card data from a PDF to sales_data database after cleaning and extracting the
        necessary information.
        """
        database_extractor = DataExtractor()
        # read card pdf to
        card_table = database_extractor.retrieve_pdf_data(
            "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        )

        data_cleaner = DataCleaning()
        # clean card table
        card_table = data_cleaner.clean_card_data(card_table)

        data_connector = DatabaseConnector()
        # connect to sales_data database
        sales_data_engine = data_connector.init_db_engine()
        # upload card_table to sales_data
        data_connector.upload_to_db(card_table, "dim_card_details", sales_data_engine)

    def upload_store_data_to_db():
        """
        This function extracts store data from an API, cleans it, and uploads it to a database.
        """
        database_extractor = DataExtractor()
        # extract store data from API into dataframe
        store_data = database_extractor.retrieve_stores_data(
            "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}"
        )

        data_cleaner = DataCleaning()
        # clean store table
        store_data = data_cleaner.clean_store_data(store_data)

        data_connector = DatabaseConnector()
        # connect to sales_data database
        sales_data_engine = data_connector.init_db_engine()
        # upload card_table to sales_data
        data_connector.upload_to_db(store_data, "dim_store_details", sales_data_engine)

    def upload_product_data_to_db():
        extractor = DataExtractor()
        # extract product data from AWS S3 bucket
        product_data = extractor.extract_from_s3("s3://data-handling-public/products.csv")
        
        data_cleaner = DataCleaning()
        # clean product data
        product_data = data_cleaner.clean_products_data(product_data)
        # convert weights to kg
        product_data = data_cleaner.convert_product_weights(product_data)
        
        data_connector = DatabaseConnector()
        # connect to sales_data database
        sales_data_engine = data_connector.init_db_engine()
        # upload products table to sales_data
        data_connector.upload_to_db(product_data, "dim_products", sales_data_engine)

    def upload_order_data_to_db():
        """
        This function uploads cleaned user data from a legacy database to sales_data database.
        """
        database_extractor = DataExtractor()
        # read credentials from yaml file
        creds_dict = database_extractor.read_db_creds("db_creds.yaml")
        # create engine 1
        db_engine = database_extractor.init_db_engine(creds_dict)
        # read user table
        order_data = database_extractor.read_rds_table("orders_table")

        data_cleaner = DataCleaning()
        # clean orders table
        order_data = data_cleaner.clean_orders_data(order_data)

        data_connector = DatabaseConnector()
        # connect to sales_data database
        sales_data_engine = data_connector.init_db_engine()
        # upload orders table to sales_data
        data_connector.upload_to_db(order_data, "orders_table", sales_data_engine)
    
    def upload_date_events_to_db():
        database_extractor = DataExtractor()
        url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
        date_events = database_extractor.download_json_s3(url)
        
        data_cleaner = DataCleaning()
        date_events = data_cleaner.clean_date_events_data(date_events)
        
        data_connector = DatabaseConnector()
        # connect to sales_data database
        sales_data_engine = data_connector.init_db_engine()
        # upload products table to sales_data
        data_connector.upload_to_db(date_events, "dim_date_times", sales_data_engine)
            
    # TODO: uncomment line below to upload user data to sales_data database
    # upload_user_data_to_db()
    # TODO: uncomment line below to upload card data to sales_data database
    # upload_card_data_to_db()
    # TODO: uncomment line below to upload store data to sales_data database
    # upload_store_data_to_db()
    # TODO: uncomment line below to upload products data to sales_data database
    # upload_product_data_to_db()
    # TODO: uncomment line below to upload orders data to sales_data database
    # upload_order_data_to_db()
    # TODO: uncomment line below to upload orders data to sales_data database
    # upload_date_events_to_db()
