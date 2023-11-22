import pandas as pd
import re

class DataCleaning():
    def __init__(self, data_frame):
        self.df = data_frame
    
    def clean_user_data(self): 
        
        # Cleaning dates
        self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth'], errors='coerce')
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], errors='coerce')

        # Cleaning phone numbers
        self.df['phone_number'] = self.df['phone_number'].str.replace('[^0-9]', '', regex=True)
        # For United Kingdom
        uk_condition = (self.df['country'] == 'United Kingdom') & ((self.df['phone_number'].str.startswith('44') & (self.df['phone_number'].str.len() == 12)) | (self.df['phone_number'].str.startswith('0') & (self.df['phone_number'].str.len() == 11)))
        # For Germany
        germany_condition = (self.df['country'] == 'Germany') & ((self.df['phone_number'].str.startswith('49') & (self.df['phone_number'].str.len() == 12)) | (self.df['phone_number'].str.len() == 10))
        # For United States
        usa_condition = (self.df['country'] == 'United States') & ((self.df['phone_number'].str.startswith('1') & (self.df['phone_number'].str.len() == 11)) | (self.df['phone_number'].str.len() == 10))
        # Concatenate the results for each country
        self.df = pd.concat([self.df[uk_condition], self.df[germany_condition], self.df[usa_condition]])
        
        # Cleaning country codes and validating them
        # Replace invalid country codes with a default value or NaN
        self.df['country_code'] = self.df['country_code'].replace('GBB', 'GB')
        valid_country_codes = ['DE', 'US', 'GB']
        self.df = self.df[self.df['country_code'].isin(valid_country_codes)]
        
        country_mapping = {'DE': 'Germany', 'US': 'United States', 'GB': 'United Kingdom'}
        self.df = self.df[self.df['country'] == self.df['country_code'].map(country_mapping)]

        # Cleaning and validating email addresses
        self.df['email_address'] = self.df['email_address'].apply(lambda x: x if '@' in x and '.' in x else None)

        # Cleaning names (allow only alphabets and spaces)
        self.df['first_name'] = self.df['first_name'].str.replace('[^a-zA-Z\s]', '', regex=True)
        self.df['last_name'] = self.df['last_name'].str.replace('[^a-zA-Z\s]', '', regex=True)

        # Handling NULL values and rows with 'NULL' string
        self.df = self.df.dropna()
        self.df = self.df[~self.df.apply(lambda row: row.astype(str).str.contains('NULL')).any(axis=1)]

        self.df = self.df.reset_index(drop=True)

        return self.df
    
    def clean_card_data(self):
        self.df['expiry_date'] = pd.to_datetime(self.df['expiry_date'], format='%m/%y', errors='coerce')
        self.df['date_payment_confirmed'] = pd.to_datetime(self.df['date_payment_confirmed'], errors='coerce')
        self.df['card_number'] = pd.to_numeric(self.df['card_number'], errors='coerce').astype('Int64')
        self.df = self.df.dropna()
        self.df = self.df[~self.df.apply(lambda row: row.astype(str).str.contains('NULL')).any(axis=1)]
        self.df = self.df.reset_index(drop=True)
        return self.df
    
    def clean_store_data(self):
        self.df['continent'] = self.df['continent'].replace('eeEurope', 'Europe')
        self.df['continent'] = self.df['continent'].replace('eeAmerica', 'America')
        country_mapping = {'DE': 'Europe', 'US': 'America', 'GB': 'Europe'}
        self.df = self.df[self.df['continent'] == self.df['country_code'].map(country_mapping)]
        valid_store_types = ['Web Portal', 'Local', 'Super Store', 'Mall Kiosk', 'Outlet']
        self.df = self.df[self.df['store_type'].isin(valid_store_types)]
        self.df['latitude'] = pd.to_numeric(self.df['latitude'], errors='coerce')
        self.df['longitude'] = pd.to_numeric(self.df['longitude'], errors='coerce')
        self.df = self.df[(self.df['latitude'] >= -90) & (self.df['latitude'] <= 90)]
        self.df = self.df[(self.df['longitude'] >= -180) & (self.df['longitude'] <= 180)]
        self.df['opening_date'] = pd.to_datetime(self.df['opening_date'], errors='coerce')
        self.df = self.df[~self.df.apply(lambda row: row.astype(str).str.contains('NULL')).any(axis=1)]
        self.df = self.df.drop('lat', axis=1)
        self.df = self.df.reset_index(drop=True)
        return self.df

    def convert_product_weights(self):
        converted_weights_in_kg = []
        for weight in self.df['weight']:
            if isinstance(weight, (int, float)):
                # If it's already a float or integer
                weight_numeric = re.sub(r'[^0-9.]', '', weight)
                converted_weights_in_kg.append(float(weight_numeric))
            elif "kg" in weight:
                # Remove 'kg' and change to float
                weight_numeric = re.sub(r'[^0-9.]', '', weight)
                converted_weights_in_kg.append(float(weight_numeric))
            elif "g" in weight:
                # Remove 'g' and change to float and divide by 1000
                weight_numeric = re.sub(r'[^0-9.]', '', weight)
                converted_weights_in_kg.append(float(weight_numeric) / 1000)
            elif "ml" in weight:
                # Remove 'ml', change to float and divide by 1000
                weight_numeric = re.sub(r'[^0-9.]', '', weight)
                converted_weights_in_kg.append(float(weight_numeric) / 1000)
            else:
                # If none of the conditions are met, append None
                converted_weights_in_kg.append(None)
        
        self.df['converted_weights_in_kg'] = converted_weights_in_kg
        return self.df

    def clean_product_data(self):
        self.df = self.convert_product_weights()
        valid_category = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy']
        self.df = self.df[self.df['category'].isin(valid_category)]
        valid_availability = ['Removed', 'Still_available']
        self.df = self.df[self.df['removed'].isin(valid_availability)]
        self.df['date_added'] = pd.to_datetime(self.df['date_added'], errors='coerce')
        self.df = self.df.dropna()
        self.df = self.df.reset_index(drop=True)
        return self.df

