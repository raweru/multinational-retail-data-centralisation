import numpy as np
import pandas as pd


# The DataCleaning class contains methods to clean and standardize user and credit card data in pandas
# dataframes.
class DataCleaning:
    
    
    def __init__(self):
        pass


    def clean_user_data(self, user_table):
        
        '''The function cleans and standardizes user data, including phone numbers, for users in different
        countries.
        
        Parameters
        ----------
        user_table
            A pandas DataFrame containing user data, with columns including "index", "first_name",
        "last_name", "date_of_birth", "join_date", "address", "country_code", and "phone_number".
        
        Returns
        -------
            a cleaned and standardized version of the input user_table, which includes removing null
        values, standardizing date formats, and standardizing phone numbers for users in the UK,
        Germany, and the US.
        '''
        
        user_table.set_index("index", inplace=True)
        user_table = user_table.replace("NULL", np.nan)
        user_table = user_table.dropna()
        user_table = user_table[~user_table['first_name'].str.contains(r'\d', na=False)]
        user_table["date_of_birth"] = pd.to_datetime(
            user_table["date_of_birth"], errors="coerce")
        user_table["date_of_birth"] = user_table["date_of_birth"].dt.strftime("%Y-%m-%d")
        user_table["join_date"] = pd.to_datetime(
            user_table["join_date"], errors="coerce")
        user_table["join_date"] = user_table["join_date"].dt.strftime("%Y-%m-%d")
        user_table["address"] = user_table["address"].str.replace("\n", ", ")
        user_table["country_code"] = user_table["country_code"].replace("GGB", "GB")

        def standardize_GB_phone_number(phone_number):
            
            '''The function standardizes a UK phone number by removing whitespace, hyphens, and dots, and
            converting it to the international format.
            
            Parameters
            ----------
            phone_number
                The input parameter is a string representing a phone number in various formats.
            
            Returns
            -------
                a standardized version of the input phone number, with all whitespace characters, hyphens,
            and dots removed, and with the country code for the United Kingdom (0044) added if
            necessary.
            '''
            
            # remove all whitespace characters
            phone_number = phone_number.replace(" ", "")
            # remove hyphens
            phone_number = phone_number.replace("-", "")
            # remove "."
            phone_number = phone_number.replace(".", "")
            # (020)74960167 to 2074960167 - remove brackets and first number, prepend 0044
            if phone_number[0] == "(":
                phone_number = phone_number.replace("(", "")
                phone_number = phone_number.replace(")", "")
                phone_number = "0044" + phone_number[1:]
            # +44(0)1164960425 to +441164960425
            if phone_number[3] == "(":
                phone_number = phone_number[0:3] + phone_number[6:]
                phone_number = "00" + phone_number[1:]
            # +44 to 0044
            if phone_number[0] == "+":
                phone_number = "00" + phone_number[1:]
            if phone_number.startswith("0") and not phone_number.startswith("0044"):
                phone_number = "0044" + phone_number[1:]

            return phone_number

        def standardize_DE_phone_number(phone_number):
            
            '''The function standardizes a German phone number by removing whitespace, hyphens, and dots,
            adding the country code if missing, and converting different formats to a consistent format.
            
            Parameters
            ----------
            phone_number
                a string representing a phone number in Germany, which may or may not be formatted
            correctly.
            
            Returns
            -------
                the standardized version of the input phone number, with all whitespace characters,
            hyphens, and dots removed, and with the appropriate country code (0049) added if necessary.
            '''
            
            # remove all whitespace characters
            phone_number = phone_number.replace(" ", "")
            # remove hyphens
            phone_number = phone_number.replace("-", "")
            # remove "."
            phone_number = phone_number.replace(".", "")
            # 049 to 0049
            if phone_number.startswith("049"):
                phone_number = "0" + phone_number
            # (020)74960167 to 2074960167 - remove brackets and first number, prepend 0044
            if phone_number[0] == "(":
                phone_number = phone_number.replace("(", "")
                phone_number = phone_number.replace(")", "")
                if phone_number.startswith("049"):
                    phone_number = "0" + phone_number
                else:
                    phone_number = "0049" + phone_number[1:]
            # 08806 869430 to 00498806869430
            if phone_number.startswith("0") and not phone_number.startswith("0049"):
                phone_number = "0049" + phone_number[1:]
            # +49(0)7133883900 to 00497133883900
            if phone_number[3] == "(":
                # remove (0)
                phone_number = phone_number[0:3] + phone_number[6:]
                # +49 to 0049
                phone_number = "00" + phone_number[1:]

            return phone_number

        def standardize_US_phone_number(phone_number):
            
            '''The function standardizes a US phone number by removing whitespace, hyphens, and dots, and
            adding the country code if necessary.
            
            Parameters
            ----------
            phone_number
                The input parameter for the function `standardize_US_phone_number()`. It is expected to be
            a string representing a phone number in various formats.
            
            Returns
            -------
                a standardized US phone number in the format "001
            '''
            
            # remove all whitespace characters
            phone_number = phone_number.replace(" ", "")
            # remove hyphens
            phone_number = phone_number.replace("-", "")
            # remove "."
            phone_number = phone_number.replace(".", "")
            # (020)74960167 to 2074960167 - remove brackets and first number, prepend 001
            if phone_number[0] == "(":
                phone_number = phone_number.replace("(", "")
                phone_number = phone_number.replace(")", "")
            # remove x and everything after it
            if "x" in phone_number:
                phone_number = phone_number.split("x")[0]
            # +1 to 001
            if phone_number[0] == "+":
                phone_number = "00" + phone_number[1:]
            # 844-345-4905 to 001844-345-4905
            if not phone_number.startswith("001"):
                phone_number = "001" + phone_number

            return phone_number

        # We iterate over rows and reassign the row to the standardized phone number version
        for index, row in user_table.iterrows():
            if row["country_code"] == "GB":
                row["phone_number"] = standardize_GB_phone_number(row["phone_number"])
            elif row["country_code"] == "DE":
                row["phone_number"] = standardize_DE_phone_number(row["phone_number"])
            elif row["country_code"] == "US":
                row["phone_number"] = standardize_US_phone_number(row["phone_number"])

        return user_table


    def clean_card_data(self, card_table):
        
        '''This function cleans and processes credit card data by removing null values, converting date
        formats, removing leading question marks from card numbers, and filtering out non-numeric card
        numbers.
        
        Parameters
        ----------
        card_table
            A pandas DataFrame containing credit card data.
        
        Returns
        -------
            a cleaned version of the input `card_table` dataframe, where "NULL" values have been replaced
        with NaN, rows with NaN values have been dropped, the "date_payment_confirmed" column has been
        converted to a datetime format and reformatted to "YYYY-MM-DD", and any rows with non-numeric
        characters in the "card_number" column have been removed.
        '''
        
        card_table = card_table.replace("NULL", np.nan)
        card_table = card_table.dropna()
        card_table["date_payment_confirmed"] = pd.to_datetime(
            card_table["date_payment_confirmed"], errors="coerce")
        card_table["date_payment_confirmed"] = card_table[
            "date_payment_confirmed"].dt.strftime("%Y-%m-%d")
        card_table = card_table[~card_table['card_number'].astype(str).str.contains('[a-zA-Z]')]

        def card_q_mark_remover(card_number):
            
            '''The function removes any leading question marks from a given card number string and returns
            the integer value of the remaining string.
            
            Parameters
            ----------
            card_number
                The input parameter is a numeric value representing a credit card number that may contain
            question marks ("?"). The function removes any leading question marks and returns the
            resulting integer value.
            
            Returns
            -------
                the card number with any leading question marks removed.
            '''
            
            card_number = str(card_number)
            while card_number[0] == "?":
                card_number = card_number[1:]
            card_number = int(card_number)

            return card_number
        
        for index, row in card_table.iterrows():
            # iterate over rows of the dataframe and remove ? from card_number
            row["card_number"] = card_q_mark_remover(row["card_number"])
        
        return card_table


    def clean_store_data(self, store_data):
        
        '''The function cleans and processes store data by replacing null values, dropping columns and
        rows, removing corrupted data, and formatting certain columns.
        
        Parameters
        ----------
        store_data
            store_data is a pandas DataFrame containing information about stores, such as their location,
        type, staff numbers, and opening date. The function clean_store_data takes this DataFrame as
        input and performs various cleaning operations on it, such as replacing certain values with NaN,
        dropping columns and rows, removing corrupted data,
        
        Returns
        -------
            the cleaned store data after performing various data cleaning operations.
        '''
        
        store_data = store_data.replace("NULL", np.nan)
        store_data = store_data.replace("N/A", np.nan)
        store_data = store_data.replace("None", np.nan)
        store_data = store_data.drop("lat", axis=1)
        store_data.drop(labels=[217, 405, 437], axis=0, inplace=True)
        # Delete country code, continent for WEB PORTAL
        store_data.loc[store_data['store_type'] == 'Web Portal', ['country_code', 'continent']] = np.nan
        # Remove rows with corrupted data
        store_data = store_data[~store_data["locality"].str.contains(r'\d', na=False)]
        store_data["address"] = store_data["address"].str.replace("\n", ", ")
        store_data["staff_numbers"] = store_data["staff_numbers"].str.replace("[a-zA-Z]", "")
        store_data["continent"] = store_data["continent"].str.replace("^ee", "")
        store_data["opening_date"] = pd.to_datetime(
            store_data["opening_date"], errors="coerce")
        store_data["opening_date"] = store_data["opening_date"].dt.strftime("%Y-%m-%d")

        return store_data


    def convert_product_weights(self, product_data):
        
        '''This function converts product weights in various units to kilograms and updates the
        product_data dataframe.
        
        Parameters
        ----------
        product_data
            a pandas dataframe containing information about products, including their weights in various
        units.
        
        Returns
        -------
            the updated product_data dataframe with weights converted to kilograms and NaN values replacing
        0.0 values.
        '''
        
        product_data["weight"] = product_data["weight"].str.replace(" .", "", regex=True)

        def product_weight_kg_converter():
            
            '''This function converts product weights in various units to kilograms and updates the
            product_data dataframe.
            '''
            
            for index, row in product_data.iterrows():
                weight = row["weight"]
                # some values are 3 x 20g, splitting them on "x", removing "g" and multiplying
                
                if "x" in weight:
                    
                    if weight.endswith("g"):
                        weight = weight[:-1]
                        substrings = weight.split("x")
                        weight = round(
                            (float(substrings[0]) * float(substrings[1]) / 1000), 2)
                        
                    elif weight.endswith("ml"):
                        weight = weight[:-2]
                        substrings = weight.split("x")
                        weight = round(
                            (float(substrings[0]) * float(substrings[1]) / 1000), 2)
                        
                elif weight.endswith("kg"):
                    weight = round((float(weight[:-2])), 2)
                    
                elif weight.endswith("g"):
                    weight = round((float(weight[:-1]) / 1000), 2)
                    
                elif weight.endswith("ml"):
                    weight = round((float(weight[:-2]) / 1000), 2)
                    
                elif weight.endswith("oz"):
                    weight = round((float(weight[:-2]) * 28.413 / 1000), 2)
                
                #update weight    
                product_data.at[index, "weight"] = weight

        product_weight_kg_converter()

        product_data.rename(columns={"weight": "weight_kg"}, inplace=True)
        product_data["weight_kg"] = product_data["weight_kg"].replace(0.0, np.nan)

        return product_data


    def clean_products_data(self, product_data):
        
        '''The function cleans and preprocesses product data by dropping certain rows, renaming columns,
        converting data types, and modifying values.
        
        Parameters
        ----------
        product_data
            a pandas DataFrame containing information about products.
        
        Returns
        -------
            the cleaned product data after performing various data cleaning operations such as dropping
        specific rows, replacing values in a column, removing rows containing digits in a specific
        column, renaming columns, removing currency symbol from a column, converting a column to
        datetime format, and formatting the date column.
        '''
        
        product_data.drop(labels=[266, 788, 794, 1660], axis=0, inplace=True)
        product_data["removed"] = product_data["removed"].replace({"Still_avaliable": False, "Removed": True})
        product_data = product_data[~product_data["category"].str.contains(r'\d', na=False)]
        product_data.rename(columns={"Unnamed: 0": "index"}, inplace=True)
        product_data.rename(columns={"product_price": "product_price_£"}, inplace=True)
        product_data["product_price_£"] = product_data["product_price_£"].str.replace("£", "")
        product_data["date_added"] = pd.to_datetime(product_data["date_added"], errors="coerce")
        product_data["date_added"] = product_data["date_added"].dt.strftime("%Y-%m-%d")

        return product_data


    def clean_orders_data(self, order_data):
        
        '''This function drops specific columns from a given order data and returns the modified data.
        
        Parameters
        ----------
        order_data
            a pandas DataFrame containing order data, with columns "level_0", "first_name", "last_name",
        and "1". The function drops these columns and returns the cleaned DataFrame.
        
        Returns
        -------
            the cleaned order data after dropping the specified columns.
        '''
        
        order_data.drop(
            labels=["level_0", "first_name", "last_name", "1"], axis=1, inplace=True)

        return order_data


    def clean_date_events_data(self, date_events):
        
        '''This function removes rows from a pandas DataFrame where the "month" column contains any
        alphabetical characters.
        
        Parameters
        ----------
        date_events
            A pandas DataFrame containing information about events and their corresponding dates. The
        DataFrame has a column named "month" which contains the month of the event as a string. The
        function is designed to clean this column by removing any rows where the month contains letters
        (i.e. non-numeric characters). The cleaned
        
        Returns
        -------
            the cleaned date_events data, which is a pandas DataFrame with the rows containing non-numeric
        characters in the "month" column removed.
        '''
        
        date_events = date_events[~date_events["month"].str.contains(r'[a-zA-Z]', na=False)]

        return date_events

