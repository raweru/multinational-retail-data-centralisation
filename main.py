from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from database_utils import DatabaseConnector


database_extractor = DataExtractor()
data_cleaner = DataCleaning()
data_connector = DatabaseConnector()


def upload_user_data_to_db():
    
    '''This function uploads cleaned user data to a database table.
    
    '''
    
    user_table = database_extractor.extract_user_data()
    user_table = data_cleaner.clean_user_data(user_table)
    data_connector.upload_to_db(user_table, "dim_users")


def upload_card_data_to_db():
    
    '''This function uploads cleaned card data from a PDF to a database table.
    
    '''
    
    card_table = database_extractor.retrieve_pdf_data(
        "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
    card_table = data_cleaner.clean_card_data(card_table)
    data_connector.upload_to_db(card_table, "dim_card_details")


def upload_store_data_to_db():
    
    '''This function retrieves store data from a database, cleans it, and uploads it to a specified table
    in a database engine.
    '''
    
    store_data = database_extractor.retrieve_stores_data(
        "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{}")
    store_data = data_cleaner.clean_store_data(store_data)
    data_connector.upload_to_db(store_data, "dim_store_details")


def upload_product_data_to_db():
    
    '''This function uploads cleaned and converted product data from an S3 bucket to a database table.
    
    '''
    
    product_data = database_extractor.extract_from_s3("s3://data-handling-public/products.csv")
    product_data = data_cleaner.clean_products_data(product_data)
    product_data = data_cleaner.convert_product_weights(product_data)
    data_connector.upload_to_db(product_data, "dim_products")


def upload_order_data_to_db():
    
    '''This function uploads cleaned order data to a database table.
    
    '''
    order_data = database_extractor.extract_order_data()
    order_data = data_cleaner.clean_orders_data(order_data)
    data_connector.upload_to_db(order_data, "orders_table")


def upload_date_events_to_db():
    
    '''This function downloads a JSON file from an S3 bucket, cleans the data, and uploads it to a database
    table.
    
    '''
    
    url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
    date_events = database_extractor.download_json_s3(url)
    date_events = data_cleaner.clean_date_events_data(date_events)
    data_connector.upload_to_db(date_events, "dim_date_times")


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
upload_date_events_to_db()