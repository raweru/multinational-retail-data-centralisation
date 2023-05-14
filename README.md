# Multinational Retail Data Centralization

You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analyzable by current members of the team. In an effort to become more data-driven, your organization would like to make its sales data accessible from one centralized location. Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralized location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business.

## Prerequisites

- numpy==1.24.3
- pandas==2.0.1
- PyYAML==6.0
- SQLAlchemy==2.0.9
- tabula_py==2.7.0
- yaml==0.2.5

## Milestone 1: Extract and clean the data from various data sources

### Task 1: Set up a new database to store the data

We used pgadmin4/postgresql to set up a new database called **sales_data**, where we centralized all the data.

### Task 2: Initialize three project classes

We defined three classes that we used for the project:

1. **DataExtractor** in data_extraction.py was used to extract data from various sources.
2. **DatabaseConnector** in database_utils.py was used to connect with and upload data to sales_data database.
3. **DataCleaning** was used to clean the data before uploading.

### Task 3: Extract and clean the user data

Legacy user data is hosted on AWS and can be accessed with the details from db_creds.yaml. We created a few methods to extract, clean and upload the user data.

1. **read_db_creds** in DatabaseExtractor reads the credentials yaml file and returns a dictionary of the credentials.
2. **init_db_engine** in DatabaseExtractor reads the credentials from the return of read_db_creds and initializes and returns an sqlalchemy database engine. We can reuse this code to create a similar method in DatabaseConnector that initializes sales_data database engine.
3. **list_db_tables** in DatabaseExtractor lists all the tables in the legacy user database so we know which tables we can extract data from.
4. **read_rds_table** in DatabaseExtractor extracts the database table to a pandas DataFrame.
5. **clean_user_data** in DatabaseCleaning performs the cleaning of the user data. Contains multiple functions needed, check their docstrings for more information.
6. **upload_to_db** in DatabaseConnector takes in a Pandas DataFrame and table name to upload to sales_data database.

Once extracted and cleaned,we used the **upload_to_db** method to store the data in sales_data database in a table named **dim_users**.

### Task 4: Extracting and cleaning users' card details

Users' card details are stored in in a pdf document [here](https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf). 

1. **retrieve_pdf_data** in DatabaseExtractor takes in a link as an argument and returns a pandas DataFrame.
2. **clean_card_data** in DataCleaning performs the cleaning of the credit card data. Contains multiple functions needed, check their docstrings for more information.

Once cleaned, we uploaded the table with **upload_to_db** method to sales_data in a table called **dim_card_details**.

