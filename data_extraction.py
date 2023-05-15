import yaml
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd
import tabula
import requests


# The `DataExtractor` class provides methods for reading database credentials from a YAML file,
# initializing a PostgreSQL database engine, listing database tables, and reading a table from a
# database as a pandas DataFrame.
class DataExtractor:
    def __init__(self):
        self.engine = None
        self.header = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}

    def read_db_creds(self, filename: str) -> dict:
        """
        This function reads a YAML file containing database credentials and returns them as a
        dictionary.

        Args:
            filename (str): The filename parameter is a string that represents the name of the file that
        contains the database credentials.

        Returns:
            A dictionary containing the database credentials read from the specified file.
        """
        with open(filename, "r") as f:
            creds = yaml.safe_load(f)

        return creds

    def init_db_engine(self, creds: dict):
        """
        This function initializes a PostgreSQL database engine using the provided credentials.

        Args:
            creds (dict): The `creds` parameter is a dictionary that contains the credentials needed to
        connect to a PostgreSQL database. It should have the following keys:

        Returns:
            The function `init_db_engine` returns the database engine created using the credentials
        provided in the `creds` dictionary.
        """
        url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        self.engine = create_engine(url)

        return self.engine

    def list_db_tables(self):
        """
        This function inspects a database engine and prints out a list of table names.
        """
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        print(table_names)

    def read_rds_table(self, table_name: str) -> pd.DataFrame:
        """
        This function reads a table from a database using the specified table name and returns it as a
        pandas DataFrame.

        Args:
            table_name (str): The name of the table in the database that you want to read.

        Returns:
            A pandas DataFrame containing the data from the specified table in the database.
        """
        with self.engine.connect() as conn:
            df = pd.read_sql_table(table_name, con=conn)

        return df

    def retrieve_pdf_data(self, link):
        """
        This function retrieves data from a PDF file using the tabula library and returns it as a pandas
        dataframe.

        Args:
            link: The link parameter is a string that represents the URL or file path of a PDF file that
        contains data to be extracted.

        Returns:
            a pandas DataFrame object named "card_table" which contains the data extracted from a PDF file
        located at the given "link" parameter.
        """
        card_tables = tabula.read_pdf(link, pages="all")
        card_table = pd.concat(card_tables)

        return card_table

    def list_number_of_stores(self, store_number_endpoint_url):
        """
        This function retrieves the number of stores from a given endpoint URL using a GET request and
        returns it as a JSON object.

        Args:
            store_number_endpoint_url: The URL endpoint for retrieving the number of stores.

        Returns:
            The method `list_number_of_stores` returns the number of stores obtained from the provided
        `store_number_endpoint_url`. If the response status code is not 200, it raises an exception with
        the corresponding error message.
        """
        response = requests.get(store_number_endpoint_url, headers=self.header)
        if response.status_code == 200:
            return response.json()["number_stores"]
        else:
            raise Exception(
                f"Failed to retrieve number of stores: {response.status_code} - {response.content}"
            )

    def retrieve_stores_data(self, retrieve_store_endpoint_url):
        """
        This function retrieves data for multiple stores and returns it as a pandas DataFrame.

        Args:
            retrieve_store_endpoint_url: The URL endpoint for retrieving data for a specific store. It is
        a string that contains a placeholder for the store number, which will be replaced with the
        actual store number during the loop.

        Returns:
            a pandas DataFrame containing data retrieved from multiple stores using an API endpoint URL.
        """
        stores_data = []
        for store_number in range(
            self.list_number_of_stores(
                "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
            )
        ):
            response = requests.get(
                retrieve_store_endpoint_url.format(store_number), headers=self.header
            )
            if response.status_code == 200:
                store_data = response.json()
                stores_data.append(store_data)
            else:
                raise Exception(
                    f"Failed to retrieve data for store {store_number}: {response.status_code} - {response.content}"
                )
        return pd.DataFrame(stores_data)
