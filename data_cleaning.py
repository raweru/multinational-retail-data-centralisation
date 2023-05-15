import numpy as np
import pandas as pd


# The DataCleaning class contains methods to clean and standardize user and credit card data in pandas
# dataframes.
class DataCleaning:
    def __init__(self):
        pass

    def clean_user_data(self, user_table):
        """
        The function cleans and standardizes user data in a pandas DataFrame, including removing corrupt
        rows, reformatting date columns, replacing newline characters in the address column, replacing a
        specific country code, and standardizing phone numbers based on country code.

        Args:
            user_table: a pandas DataFrame containing user data, with columns such as "index",
        "first_name", "last_name", "date_of_birth", "join_date", "address", "country_code", and
        "phone_number". The function aims to clean and standardize the data in this table.

        Returns:
            a cleaned and standardized version of the input user_table dataframe, with null values and
        corrupt rows removed, date and address columns reformatted, and phone numbers standardized
        according to their respective country codes.
        """
        # set "index" column as the index
        user_table.set_index("index", inplace=True)

        # 21 rows with only index and other columns all NULL
        user_table = user_table.replace("NULL", np.nan)

        # drop rows with null values
        user_table = user_table.dropna()

        def user_corrupt_row_remover():  # some rows have random numbers in all rows
            """
            The function removes rows from a user table where the first name contains any special
            characters or numbers.
            """
            values = []
            for name in user_table["first_name"]:
                for letter in name:
                    if letter in "0123456789!#$%&'()*+,/:;?@[\]^_`{|}~":
                        values.append(name)
                        break
            indices = user_table[user_table["first_name"].isin(values)].index
            user_table.drop(indices, inplace=True)

        def user_datetime_formatter():
            """
            This function converts and reformats date columns in a pandas dataframe to a consistent
            format.
            """
            user_table["date_of_birth"] = pd.to_datetime(
                user_table["date_of_birth"], errors="coerce"
            )
            # reformat the date column to YYYY-MM-DD, BD
            user_table["date_of_birth"] = user_table["date_of_birth"].dt.strftime(
                "%Y-%m-%d"
            )
            # convert the date column to datetime format, JD
            user_table["join_date"] = pd.to_datetime(
                user_table["join_date"], errors="coerce"
            )
            # reformat the date column to YYYY-MM-DD, JD
            user_table["join_date"] = user_table["join_date"].dt.strftime("%Y-%m-%d")

        def user_address_formatter():
            """
            This function replaces newline characters in the "address" column of a pandas DataFrame with
            commas and spaces.
            """
            user_table["address"] = user_table["address"].str.replace("\n", ", ")

        def user_country_code_GGB_to_GB():
            """
            This function replaces the country code "GGB" with "GB" in a user table.
            """
            # country_code GGB instead GB
            user_table["country_code"] = user_table["country_code"].replace("GGB", "GB")

        def standardize_GB_phone_number(phone_number):
            """
            The function standardizes a phone number in the format used in Great Britain by removing
            whitespace, hyphens, and dots, and converting it to the international format with the
            country code 0044.

            Args:
                phone_number: The input parameter is a string representing a phone number in the format
            used in Great Britain (GB).

            Returns:
                a standardized version of the input phone number in the format used in Great Britain (GB).
            """
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
            """
            The function standardizes German phone numbers by removing whitespace, hyphens, and dots,
            and adding the country code 0049 if necessary.

            Args:
                phone_number: The input parameter for the function standardize_DE_phone_number, which is
            expected to be a phone number in Germany.

            Returns:
                the standardized version of the input phone number according to the rules specified in the
            code.
            """
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
            """
            The function standardizes a US phone number by removing whitespace, hyphens, and dots, and
            adding the country code if necessary.

            Args:
                phone_number: The input parameter is a string representing a phone number in the US
            format.

            Returns:
                a standardized US phone number in the format "001XXX-XXX-XXXX".
            """
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

        user_corrupt_row_remover()
        user_datetime_formatter()
        user_address_formatter()
        user_country_code_GGB_to_GB()

        # We'll iterate over rows of the dataframe and reassign the row to the standardized phone number version
        for index, row in user_table.iterrows():
            if row["country_code"] == "GB":
                row["phone_number"] = standardize_GB_phone_number(row["phone_number"])
            elif row["country_code"] == "DE":
                row["phone_number"] = standardize_DE_phone_number(row["phone_number"])
            elif row["country_code"] == "US":
                row["phone_number"] = standardize_US_phone_number(row["phone_number"])

        return user_table

    def clean_card_data(self, card_table):
        """
        The function cleans and formats credit card data in a pandas dataframe by removing null values,
        corrupt rows, extra digits and question marks from the card provider and number columns, and
        reformatting the date column.

        Args:
            card_table: The input parameter `card_table` is a pandas dataframe containing credit card
        data.

        Returns:
            a cleaned version of the input `card_table` dataframe, with null values dropped, date columns
        reformatted, corrupt rows removed, and certain characters removed from the `card_provider` and
        `card_number` columns.
        """
        # replace 'NULL' string with null
        card_table = card_table.replace("NULL", np.nan)

        # drop rows with null values
        card_table = card_table.dropna()

        def card_datetime_formatter():
            """
            This function converts a date column in a pandas dataframe to datetime format and reformats
            it to YYYY-MM-DD.
            """
            card_table["date_payment_confirmed"] = pd.to_datetime(
                card_table["date_payment_confirmed"], errors="coerce"
            )
            # reformat the date column to YYYY-MM-DD
            card_table["date_payment_confirmed"] = card_table[
                "date_payment_confirmed"
            ].dt.strftime("%Y-%m-%d")

        # some rows have random numbers in all rows
        def card_corrupt_row_remover():
            """
            This function removes rows from a card table where the expiry date contains non-numeric
            characters except forward slash.
            """
            values = []
            for name in card_table["expiry_date"]:
                for letter in name:
                    if (
                        letter
                        in "qwertyuioplkjhgfdsazxcvbnmQWERTYUIOPLKJHGFDSAZXCVBNM!#$%&'()*+,:;?@[\]^_`{|}~"
                    ):
                        values.append(name)
                        break
            indices = card_table[card_table["expiry_date"].isin(values)].index
            card_table.drop(indices, inplace=True)

        # drop "X digits" from card_provider
        def card_x_digit_remover(provider):
            """
            This function removes the "digit" word from a string if it is present.

            Args:
                provider: The input parameter "provider" is a string that represents the name of a credit
            card provider.

            Returns:
                the modified string `provider` with the word "digit" removed if it is present in the
            string.
            """
            if "digit" in provider:
                provider = provider.split(" ")[0]

            return provider

        # some VISA card numbers have 19 digits, have 000 at the end that needs removing
        def card_zeros_digit_remover(card_number):
            """
            The function removes the last three zeros from a given card number if it is longer than 16
            digits.

            Args:
                card_number: The parameter `card_number` is a numeric value representing a credit card
            number.

            Returns:
                the modified card number with the trailing zeros removed. If the original card number had
            a length greater than 16 and ended with "000", the function removes the last three digits
            and returns the updated card number as an integer. If the original card number did not meet
            these conditions, the function returns the original card number as is.
            """
            card_number = str(card_number)
            if len(card_number) > 16 and card_number[-3:] == "000":
                card_number = card_number[:-3]
                card_number = int(card_number)

            return card_number

        # some card numbers start with a few ? marks
        def card_q_mark_remover(card_number):
            """
            The function removes any leading question marks from a given card number string and returns
            the integer value of the remaining string.

            Args:
                card_number: The input parameter is a numeric value representing a credit card number that
            may contain question marks ("?"). The function removes any leading question marks and
            returns the resulting integer value.

            Returns:
                the card number with any leading question marks removed.
            """
            card_number = str(card_number)
            while card_number[0] == "?":
                card_number = card_number[1:]
            card_number = int(card_number)

            return card_number

        card_datetime_formatter()
        card_corrupt_row_remover()

        for index, row in card_table.iterrows():
            # iterate over rows of the dataframe and remove "X digits" from card_provider
            row["card_provider"] = card_x_digit_remover(row["card_provider"])
            # iterate over rows of the dataframe and remove extra zeros
            row["card_number"] = card_zeros_digit_remover(row["card_number"])
            # iterate over rows of the dataframe and remove ? from card_number
            row["card_number"] = card_q_mark_remover(row["card_number"])

        return card_table

    def clean_store_data(self, store_data):
        """
        This function cleans and formats data in a pandas DataFrame for a store dataset.

        Args:
            store_data: a pandas DataFrame containing information about stores, such as their addresses,
        staff numbers, opening dates, and continents. The function aims to clean and format the data in
        this DataFrame by removing missing values, formatting staff numbers and addresses, and
        converting the opening date column to datetime format.

        Returns:
            the cleaned store_data dataframe after applying various data cleaning operations such as
        replacing null values, dropping columns and rows with missing values, removing non-numeric
        characters from staff numbers, removing "ee" from continent values, and reformatting date
        columns to YYYY-MM-DD format.
        """
        # replace 'NULL' string with null
        store_data = store_data.replace("NULL", np.nan)

        # replace 'N/A' string with null
        store_data = store_data.replace("N/A", np.nan)

        # replace 'None' string with null
        store_data = store_data.replace("None", np.nan)

        # Drop lat column as only has 7 non null values
        store_data = store_data.drop("lat", axis=1)

        # Drop rows that have NULL in all columns
        store_data.drop(labels=[217, 405, 437], axis=0, inplace=True)

        # some rows have random numbers in all rows
        def store_corrupt_row_remover():
            """
            This function removes rows from a card table where the expiry date contains non-numeric
            characters except forward slash.
            """
            values = []
            for name in store_data["locality"]:
                if pd.notnull(name):  # Check if the value is not NaN
                    for letter in name:
                        if letter in "1234567890!#$%&'()*+,:;?@[\]^_`{|}~":
                            values.append(name)
                            break
            indices = store_data[store_data["locality"].isin(values)].index
            store_data.drop(indices, inplace=True)

        def store_address_formatter():
            """
            This function replaces newline characters in the "address" column of a pandas DataFrame with
            commas and spaces.
            """
            store_data["address"] = store_data["address"].str.replace("\n", ", ")

        # some staff numbers include letters
        def store_staff_number_formatter():
            """
            The function removes any alphabetical characters from the 'staff_numbers' column in the
            'store_data' dataframe.
            """
            store_data["staff_numbers"] = store_data["staff_numbers"].str.replace(
                "[a-zA-Z]", ""
            )

        # some continent values eeAmerica/eeEurope need ee removed
        def store_ee_continent_remover():
            """
            This function removes the string "ee" from the beginning of each value in a specific column
            of a pandas DataFrame called "store_data".
            """
            store_data["continent"] = store_data["continent"].str.replace("^ee", "")

        def store_datetime_formatter():
            """
            This function converts a date column in a pandas dataframe to datetime format and reformats
            it to YYYY-MM-DD.
            """
            store_data["opening_date"] = pd.to_datetime(
                store_data["opening_date"], errors="coerce"
            )
            # reformat the date column to YYYY-MM-DD
            store_data["opening_date"] = store_data["opening_date"].dt.strftime(
                "%Y-%m-%d"
            )

        store_datetime_formatter()
        store_ee_continent_remover()
        store_staff_number_formatter()
        store_address_formatter()
        store_corrupt_row_remover()

        return store_data

    def convert_product_weights(self, product_data):
        """
        This function converts product weights in various units to decimal values in kg and renames the
        weight column as weight_kg while replacing 0.0 values with np.nan.

        Args:
            product_data: a pandas DataFrame containing information about products, including their
        weights in various units.

        Returns:
            the modified product_data dataframe with weights converted to decimal values in kg, the weight
        column renamed to weight_kg, and any 0.0 values in the weight column replaced with np.nan.
        """
        # one value is "77g .", removing " ."
        product_data["weight"] = product_data["weight"].str.replace(
            " .", "", regex=True
        )

        # Convert weights to decimal values in kg
        def product_weight_kg_converter():
            """
            This function converts product weights in various units to kilograms and updates the
            product_data dataframe.
            """
            for index, row in product_data.iterrows():
                weight = row["weight"]
                # some values are 3 x 20g, splitting them on "x", removing "g" and multiplying
                if "x" in weight:
                    if weight.endswith("g"):
                        weight = weight[:-1]
                        substrings = weight.split("x")
                        weight = round(
                            (float(substrings[0]) * float(substrings[1]) / 1000), 2
                        )
                    elif weight.endswith("ml"):
                        weight = weight[:-2]
                        substrings = weight.split("x")
                        weight = round(
                            (float(substrings[0]) * float(substrings[1]) / 1000), 2
                        )
                elif weight.endswith("kg"):
                    weight = round((float(weight[:-2])), 2)
                elif weight.endswith("g"):
                    weight = round((float(weight[:-1]) / 1000), 2)
                elif weight.endswith("ml"):
                    weight = round((float(weight[:-2]) / 1000), 2)
                elif weight.endswith("oz"):
                    weight = round((float(weight[:-2]) * 28.413 / 1000), 2)
                product_data.at[index, "weight"] = weight

        product_weight_kg_converter()

        # rename column as weight_kg
        product_data.rename(columns={"weight": "weight_kg"}, inplace=True)

        # some products have value 0.0, changed to np.nan
        product_data["weight_kg"] = product_data["weight_kg"].replace(0.0, np.nan)

        return product_data

    def clean_products_data(self, product_data):
        """
        The function cleans and formats product data by dropping null rows, removing corrupt rows,
        renaming columns, formatting prices, and converting date columns to datetime format.

        Args:
            product_data: A pandas dataframe containing product data.

        Returns:
            the cleaned product data after performing various data cleaning operations such as dropping
        rows with NULL values, removing rows with non-numeric characters in the expiry date, renaming
        columns, formatting the date column to datetime format, and removing the £ sign from the product
        price column.
        """
        # Drop rows that have NULL in all columns
        product_data.drop(labels=[266, 788, 794, 1660], axis=0, inplace=True)

        # some rows have random numbers in all rows
        def product_corrupt_row_remover():
            """
            This function removes rows from a card table where the expiry date contains non-numeric
            characters except forward slash.
            """
            values = []
            for name in product_data["category"]:
                if pd.notnull(name):  # Check if the value is not NaN
                    for letter in name:
                        if letter in "1234567890":
                            values.append(name)
                            break
            indices = product_data[product_data["category"].isin(values)].index
            product_data.drop(indices, inplace=True)

        # rename Unnamed: 0 column to index
        product_data.rename(columns={"Unnamed: 0": "index"}, inplace=True)

        # remove £ sign from prices and add to column name
        def product_price_formatter():
            """
            This function renames a column in a dataframe and removes the pound sign from the values in
            that column.
            """
            product_data.rename(
                columns={"product_price": "product_price_£"}, inplace=True
            )
            product_data["product_price_£"] = product_data[
                "product_price_£"
            ].str.replace("£", "")

        def product_datetime_formatter():
            """
            This function converts a date column in a pandas dataframe to datetime format and reformats
            it to YYYY-MM-DD.
            """
            product_data["date_added"] = pd.to_datetime(
                product_data["date_added"], errors="coerce"
            )
            # reformat the date column to YYYY-MM-DD
            product_data["date_added"] = product_data["date_added"].dt.strftime(
                "%Y-%m-%d"
            )

        product_datetime_formatter()
        product_corrupt_row_remover()
        product_price_formatter()

        return product_data

    def clean_orders_data(self, order_data):
        # remove  columns level_0, first_name, last_name and 1
        order_data.drop(
            labels=["level_0", "first_name", "last_name", "1"], axis=1, inplace=True
        )

        return order_data

    def clean_date_events_data(self, date_events):
        """
        This function removes rows from a dataframe where the "month" column contains any
        non-alphanumeric characters.

        Args:
            date_events: a pandas dataframe containing date and event information.

        Returns:
            The function `clean_date_events_data` is returning the cleaned `date_events` dataframe after
        removing corrupted rows where the "month" column contains any non-alphanumeric characters.
        """

        # remove corrupted rows and rows with all null values
        def events_corrupt_row_remover():
            """
            This function removes rows from a dataframe where the "month" column contains any
            non-alphanumeric characters.
            """
            
            values = []
            for name in date_events["month"]:
                for letter in name:
                    if (
                        letter
                        in "qwertyuiopasdfghjklmnbvcxzQWERTYUIOPLKJHGFDSAZXCVBNM!#$%&'()*+,/:;?@[\]^_`{|}~"
                    ):
                        values.append(name)
                        break
            indices = date_events[date_events["month"].isin(values)].index
            date_events.drop(indices, inplace=True)

        events_corrupt_row_remover()

        return date_events

    