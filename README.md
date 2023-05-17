# Multinational Retail Data Centralization

You work for a multinational company that sells various goods across the globe. Currently, their sales data is spread across many different data sources making it not easily accessible or analyzable by current members of the team. In an effort to become more data-driven, your organization would like to make its sales data accessible from one centralized location. Your first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralized location and acts as a single source of truth for sales data. You will then query the database to get up-to-date metrics for the business.

## Prerequisites

- numpy==1.24.3
- pandas==2.0.1
- PyYAML==6.0
- SQLAlchemy==2.0.9
- tabula_py==2.7.0
- yaml==0.2.5
- Requests==2.30.0
- boto3==1.26.133

## Milestone 1: Extract and clean the data from various data sources

First mission is to extract all the data from the multitude of data sources, clean it, and then store it in a new database we create.

### Task 1: Set up a new database to store the data

We use pgadmin4/postgresql to set up a new database called **sales_data**, where we centralize all the data.

### Task 2: Initialize three project classes

We define three classes that we use for the project:

1. **DataExtractor** in **data_extraction.py** extracts data from various sources.
2. **DatabaseConnector** in **database_utils.py** connects with and uploads data to sales_data database.
3. **DataCleaning** in **data_cleaning.py** cleans the data before uploading.

We imported all classes into database_utils.py, which was used to upload the data streams into the new database one by one. cleaner.ipynb notebook file was used to do the cleaning before writing the final cleaning methods.

### Task 3: Extract and clean the user data

Legacy user data is hosted on AWS and can be accessed with the details from db_creds.yaml. We create a few methods to extract, clean and upload the user data.

1. **read_db_creds** in DatabaseExtractor reads the credentials yaml file and returns a dictionary of the credentials.
2. **init_db_engine** in DatabaseExtractor reads the credentials from the return of read_db_creds and initializes and returns an sqlalchemy database engine. We can reuse this code to create a similar method in DatabaseConnector that initializes sales_data database engine.
3. **list_db_tables** in DatabaseExtractor lists all the tables in the legacy user database so we know which tables we can extract data from.
4. **read_rds_table** in DatabaseExtractor extracts the database table to a pandas DataFrame.
5. **clean_user_data** in DatabaseCleaning performs the cleaning of the user data. Contains multiple functions needed, check their docstrings for more information.
6. **upload_to_db** in DatabaseConnector takes in a Pandas DataFrame and table name to upload to sales_data database.

Once extracted and cleaned,we use the **upload_to_db** method to store the data in sales_data database in a table named **dim_users**.

### Task 4: Extract and clean users' card details

Users' card details are stored in in a pdf document [here](https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf). We create a few methods to extract, clean and upload the card data.

1. **retrieve_pdf_data** in DatabaseExtractor takes in a link as an argument and returns a pandas DataFrame.
2. **clean_card_data** in DataCleaning performs the cleaning of the credit card data. Contains multiple functions needed, check their docstrings for more information.

Once extracted and cleaned, we upload the table with **upload_to_db** method to sales_data in a table called **dim_card_details**.

### Task 5: Extract and clean store details

The store data can be retrieved through the use of an API. The API has two GET methods. One will return the number of stores in the business and the other to retrieve a store given a store number.

To connect to the API we need to include the API key to connect to the API in the method header. The two endpoints for the API are as follows:

- Retrieve a store: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}
- Return the number of stores: https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores

We create the header dict and a few methods to extract, clean and upload the store data.

1. Create a dictionary to store the header details. It will have a key **x-api-key** with the value **yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX**.
2. **list_number_of_stores** in DataExtractor returns the number of stores to extract. It should take in the "Number of stores" endpoint and header dictionary as an argument.
3. **retrieve_stores_data** in DataExtractor takes the "Retrieve a store" endpoint as an argument and extracts all the stores from the API saving them in a pandas DataFrame.
4. **clean_store_data** in DataCleaning cleans the data retrieved from the API and returns a pandas DataFrame.

Once extracted and cleaned, we upload the table with **upload_to_db** method to sales_data in a table called **dim_store_details**.

### Task 6: Extract and clean the product details

The information for each product the company currently sells is stored in CSV format in an S3 bucket on AWS. The S3 address for the products data is the following: **s3://data-handling-public/products.csv**.

We create a few methods to extract, clean and upload the products data.

1. **extract_from_s3** in DataExtractor uses the boto3 package to download and extract the information returning a pandas DataFrame. We need to be logged into the AWS CLI before retrieving the data from the bucket.
2. **convert_product_weights** in DataCleaning takes the products DataFrame as an argument and converts all different weight units and formats into kg decimal value.
3. **clean_products_data** in DataCleaning cleans the rest of the data and returns a pandas DataFrame.

Once extracted and cleaned, we upload the table with **upload_to_db** method to sales_data in a table called **dim_products**.

### Task 7: Retrieve and clean the orders table

This table which acts as the single source of truth for all orders the company has made in the past is stored in a database on AWS RDS.

First, we can reuse **list_db_tables** and **read_rds_table** methods to extract the orders table.

Next, we create **clean_orders_data** to remove columns "order_0", "first_name", "last_name" and "1".

Once extracted and cleaned, we upload the table with **upload_to_db** method to sales_data in a table called **orders_table**.

### Task 8: Retrieve and clean the date events data

The final source of data is a JSON file containing the details of when each sale happened, as well as related attributes.

The file is currently stored on S3 and can be found at the following [link](https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json).

We create a few methods to extract, clean and upload the date events data.

1. **download_json_s3** in DataExtractor downloads the json file from S3 into a pandas Dataframe.
2. **clean_date_events_data** in DataCleaning to remove rows with null and corrupt data.

Once extracted and cleaned, we upload the table with **upload_to_db** method to sales_data in a table called **dim_date_times**.

## Milestone 3: Create the database schema

Now it's all about casting all columns to proper data types and connecting the tables with primary and foreign keys.

**Primary key setup template:**

```sql
ALTER TABLE dim_store_details
ADD CONSTRAINT pk_dim_store_details PRIMARY KEY (store_code);
```

**Foreign key setup template:**

```sql
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_code
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code);
```
