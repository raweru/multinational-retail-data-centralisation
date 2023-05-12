import numpy as np
import pandas as pd

class DataCleaning:
    def __init__(self):
        pass
    
    def clean_user_data(self, user_table):
        user_table.set_index("index", inplace=True)
        # 21 rows with only index and other columns all NULL
        user_table = user_table.replace("NULL", np.nan)
        # drop rows with null values
        user_table = user_table.dropna()
        # some rows have random numbers in all rows
        values = []
        for name in user_table["first_name"]:
            for letter in name:
                if letter in "0123456789!#$%&'()*+,/:;?@[\]^_`{|}~":
                    values.append(name)
                    break
        #user_table[user_table['first_name'].isin(values)]
        indices = user_table[user_table['first_name'].isin(values)].index
        user_table.drop(indices, inplace=True)
        # convert the date column to datetime format, BD
        user_table['date_of_birth'] = pd.to_datetime(user_table['date_of_birth'], errors='coerce')
        # reformat the date column to YYYY-MM-DD, BD
        user_table['date_of_birth'] = user_table['date_of_birth'].dt.strftime('%Y-%m-%d')
        # convert the date column to datetime format, JD           
        user_table['join_date'] = pd.to_datetime(user_table['join_date'], errors='coerce')
        # reformat the date column to YYYY-MM-DD, JD
        user_table['join_date'] = user_table['join_date'].dt.strftime('%Y-%m-%d')
        # address change \n to ,
        user_table['address'] = user_table['address'].str.replace('\n', ', ')
        # country_code GGB instead GB
        user_table["country_code"] = user_table["country_code"].replace("GGB", "GB")
        # standardize phone number
        def standardize_GB_phone_number(phone_number):
            # GB NUMBERS
            # remove all whitespace characters
            phone_number = phone_number.replace(' ', '')
            # remove hyphens
            phone_number = phone_number.replace('-', '')
            # remove "."
            phone_number = phone_number.replace('.', '')
            # (020)74960167 to 2074960167 - remove brackets and first number, prepend 0044
            if phone_number[0] == '(':
                phone_number = phone_number.replace('(', '')
                phone_number = phone_number.replace(')', '')
                phone_number = "0044" + phone_number[1:]
            # +44(0)1164960425 to +441164960425
            if phone_number[3] == '(':
                phone_number = phone_number[0:3] + phone_number[6:]
                phone_number = "00" + phone_number[1:]
            # +44 to 0044
            if phone_number[0] == '+':
                phone_number = "00" + phone_number[1:]
            if phone_number.startswith("0") and not phone_number.startswith("0044"):
                phone_number = "0044" + phone_number[1:]
            
            return phone_number
        
        def standardize_DE_phone_number(phone_number):        
            # DE NUMBERS
            # remove all whitespace characters
            phone_number = phone_number.replace(' ', '')
            # remove hyphens
            phone_number = phone_number.replace('-', '')
            # remove "."
            phone_number = phone_number.replace('.', '')
            #049 to 0049
            if phone_number.startswith("049"):
                phone_number = "0" + phone_number
            # (020)74960167 to 2074960167 - remove brackets and first number, prepend 0044
            if phone_number[0] == '(':
                phone_number = phone_number.replace('(', '')
                phone_number = phone_number.replace(')', '')
                if phone_number.startswith("049"):
                    phone_number = "0" + phone_number
                else:
                    phone_number = "0049" + phone_number[1:]
            # 08806 869430 to 00498806869430
            if phone_number.startswith("0") and not phone_number.startswith("0049"):
                phone_number = "0049" + phone_number[1:]
            # +49(0)7133883900 to 00497133883900
            if phone_number[3] == '(':
                # remove (0)
                phone_number = phone_number[0:3] + phone_number[6:]
                # +49 to 0049
                phone_number = "00" + phone_number[1:]
        
            return phone_number
        
        def standardize_US_phone_number(phone_number):
            # US NUMBERS
            # remove all whitespace characters
            phone_number = phone_number.replace(' ', '')
            # remove hyphens
            phone_number = phone_number.replace('-', '')
            # remove "."
            phone_number = phone_number.replace('.', '')
            # (020)74960167 to 2074960167 - remove brackets and first number, prepend 001
            if phone_number[0] == '(':
                phone_number = phone_number.replace('(', '')
                phone_number = phone_number.replace(')', '')
            #remove x and everything after it
            if "x" in phone_number:
                phone_number = phone_number.split("x")[0]
            # +1 to 001
            if phone_number[0] == '+':
                phone_number = "00" + phone_number[1:]
            # 844-345-4905 to 001844-345-4905
            if not phone_number.startswith("001"):
                phone_number = "001" + phone_number
            
            return phone_number
        
        # We'll iterate over rows of the dataframe and reassign the row to the standardized version
        for index, row in user_table.iterrows():
            if row["country_code"] == "GB":
                row["phone_number"] = standardize_GB_phone_number(row["phone_number"])
            elif row["country_code"] == "DE":
                row["phone_number"] = standardize_DE_phone_number(row["phone_number"])
            elif row["country_code"] == "US":
                row["phone_number"] = standardize_US_phone_number(row["phone_number"])
        
        return user_table

