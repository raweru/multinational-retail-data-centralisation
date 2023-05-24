import yaml
from sqlalchemy import create_engine
import pandas as pd
import tabula
import requests
import boto3
import json


# The `DataExtractor` class provides methods for reading database credentials from a YAML file,
# initializing a PostgreSQL database engine, listing database tables, and reading a table from a
# database as a pandas DataFrame.
class DataExtractor:
    
    
    def __init__(self):
        self.engine = None


    def read_db_creds(self, filename: str) -> dict:
        
        '''This function reads database credentials from a YAML file and returns them as a dictionary.
        
        Parameters
        ----------
        filename : str
            The filename parameter is a string that represents the name of the file that contains the
        database credentials.
        
        Returns
        -------
            A dictionary containing the database credentials read from the specified file.
        '''
        file_path = "ignore_these/" + filename
        with open(file_path, "r") as f:
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
            a SQLAlchemy engine object that is created using the credentials provided in the `creds`
        dictionary.
        '''
        
        url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        self.engine = create_engine(url)

        return self.engine


    def read_rds_table(self, table_name: str) -> pd.DataFrame:
        
        '''This function reads a table from an RDS database and returns it as a pandas DataFrame.
        
        Parameters
        ----------
        table_name : str
            The name of the table in the database that you want to read.
        
        Returns
        -------
            A pandas DataFrame containing the data from the specified table in the database connected to by
        the engine object.
        '''
        
        with self.engine.connect() as conn:
            df = pd.read_sql_table(table_name, con=conn)

        return df


    def retrieve_pdf_data(self, link):
        
        '''This function retrieves data from a PDF file using the tabula library in Python.
        
        Parameters
        ----------
        link
            The link parameter is a string that represents the URL or file path of a PDF file that contains
        data to be extracted.
        
        Returns
        -------
            a pandas DataFrame object named "card_table" which contains data extracted from a PDF file
        located at the "link" parameter using the tabula library.
        '''
        
        card_tables = tabula.read_pdf(link, pages="all")
        card_table = pd.concat(card_tables)

        return card_table


    def list_number_of_stores(self, store_number_endpoint_url):
        
        '''This function retrieves the number of stores from a given endpoint URL using a header file.
        
        Parameters
        ----------
        store_number_endpoint_url
            The URL endpoint for retrieving the number of stores.
        
        Returns
        -------
            the number of stores obtained from the provided store_number_endpoint_url.
        '''
        
        config_file = 'ignore_these/header.yaml'
        
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)

        header = config['header']
        response = requests.get(store_number_endpoint_url, headers=header)        
        
        if response.status_code == 200:
            
            return response.json()["number_stores"]
    
        else:
            raise Exception(
                f"Failed to retrieve number of stores: {response.status_code} - {response.content}")


    def retrieve_stores_data(self, retrieve_store_endpoint_url):
        
        '''This function retrieves data for multiple stores using an API endpoint and returns it as a
        pandas DataFrame.
        
        Parameters
        ----------
        retrieve_store_endpoint_url
            The URL endpoint used to retrieve data for each store. It is likely a string with a placeholder
        for the store number, which is filled in using the `format()` method.
        
        Returns
        -------
            a pandas DataFrame containing data retrieved from a list of stores using an API endpoint URL
        and a header.
        '''
        
        stores_data = []
        
        config_file = 'ignore_these/header.yaml'
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)

        header = config['header']
        
        for store_number in range(
            self.list_number_of_stores(
                "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores")):
            
            response = requests.get(
                retrieve_store_endpoint_url.format(store_number), headers=header)
            
            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
            
            else:
                raise Exception(
                    f"Failed to retrieve data for store {store_number}: {response.status_code} - {response.content}")
        
        return pd.DataFrame(stores_data)


    def extract_from_s3(self, s3_address):
        
        '''This function downloads a CSV file from an S3 bucket and returns it as a pandas dataframe.
        
        Parameters
        ----------
        s3_address
            The parameter `s3_address` is a string that represents the address of a file stored in an
        Amazon S3 bucket. The format of the string should be "s3://bucket_name/file_path".
        
        Returns
        -------
            a pandas DataFrame that contains the data from the CSV file downloaded from the specified S3
        address.
        '''
        
        bucket_name, file_path = s3_address.replace("s3://", "").split("/", 1)

        s3 = boto3.client("s3")
        s3.download_file(bucket_name, file_path, "products.csv")

        df = pd.read_csv("products.csv")

        return df


    def download_json_s3(self, s3_link):
        
        '''This function downloads a JSON file from an S3 link, converts it to a pandas DataFrame, and
        returns the DataFrame.
        
        Parameters
        ----------
        s3_link
            The parameter `s3_link` is a string that represents the link to a JSON file stored in an Amazon
        S3 bucket. The function downloads the JSON file from the S3 bucket, converts it to a pandas
        DataFrame, and returns the DataFrame.
        
        Returns
        -------
            a pandas DataFrame that contains the data from a JSON file downloaded from an S3 link.
        '''
        
        url = s3_link
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data)
        
        return df
    
    
    def extract_user_data(self):
        
        '''This function extracts user data from a legacy_users table in a database using credentials from
        a YAML file.
        
        Returns
        -------
            the data from the "legacy_users" table in the database specified by the credentials in the
        "db_creds.yaml" file.
        '''
        
        creds_dict = self.read_db_creds("db_creds.yaml")
        db_engine = self.init_db_engine(creds_dict)
        user_table = self.read_rds_table("legacy_users")

        return user_table
    
    
    def extract_order_data(self):
        
        '''This function extracts order data from an RDS table using credentials from a YAML file.
        
        Returns
        -------
            the data from the "orders_table" in the RDS database.
        '''
        
        creds_dict = self.read_db_creds("db_creds.yaml")
        db_engine = self.init_db_engine(creds_dict)
        order_data = self.read_rds_table("orders_table")
        
        return order_data