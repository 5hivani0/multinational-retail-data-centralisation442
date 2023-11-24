import pandas as pd
import re
import uuid

class DataCleaning():
    """
    Class for cleaning and transforming data in a DataFrame.

    Attributes:
        df (pd.DataFrame): DataFrame to be cleaned and transformed.
    """
    def __init__(self, data_frame):
        """
        Initializes a new instance of the DataCleaning class.

        Args:
            data_frame (pd.DataFrame): DataFrame to be cleaned and transformed.
        """
        self.df = data_frame
    
    def clean_user_data(self): 
        """
        Clean and transform user data in the DataFrame.

        Returns:
            pd.DataFrame: Cleaned and transformed DataFrame containing user data.
        """
        # Cleaning dates
        self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth'], errors='coerce')
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], errors='coerce')

        # Cleaning country codes and validating them
        # Replace invalid country codes with a default value or NaN
        self.df['country_code'] = self.df['country_code'].replace('GBB', 'GB')
        valid_country_codes = ['DE', 'US', 'GB']
        self.df = self.df[self.df['country_code'].isin(valid_country_codes)]
        
        self.df = self.df.dropna()

        return self.df
    
    def clean_card_data(self):
        """
        Clean and transform card data in the DataFrame.

        Returns:
            pd.DataFrame: Cleaned and transformed DataFrame containing card data.
        """
        self.df['expiry_date'] = pd.to_datetime(self.df['expiry_date'], format='%m/%y', errors='coerce')
        self.df['date_payment_confirmed'] = pd.to_datetime(self.df['date_payment_confirmed'], errors='coerce')
        self.df['card_number'] = pd.to_numeric(self.df['card_number'], errors='coerce').astype('Int64')
        self.df = self.df.dropna()
        self.df = self.df[~self.df.apply(lambda row: row.astype(str).str.contains('NULL')).any(axis=1)]
        self.df = self.df.reset_index(drop=True)
        return self.df
    
    def clean_store_data(self):
        """
        Clean and transform store data in the DataFrame.

        Returns:
            pd.DataFrame: Cleaned and transformed DataFrame containing store data.
        """
        self.df['continent'] = self.df['continent'].replace('eeEurope', 'Europe')
        self.df['continent'] = self.df['continent'].replace('eeAmerica', 'America')
        country_mapping = {'DE': 'Europe', 'US': 'America', 'GB': 'Europe'}
        self.df = self.df[self.df['continent'] == self.df['country_code'].map(country_mapping)]
        valid_store_types = ['Web Portal', 'Local', 'Super Store', 'Mall Kiosk', 'Outlet']
        self.df = self.df[self.df['store_type'].isin(valid_store_types)]
        self.df['opening_date'] = pd.to_datetime(self.df['opening_date'], errors='coerce')
        self.df['staff_numbers'] = self.df['staff_numbers'].replace('[^\d]', '', regex=True)
        self.df = self.df.drop('lat', axis=1)
        self.df = self.df.reset_index(drop=True)
        return self.df

    def convert_product_weights(self):
        """
        Convert product weights to a consistent unit (kilograms).

        Returns:
            pd.DataFrame: DataFrame with weights converted to kilograms.
        """
        converted_weights_in_kg = []
        for weight in self.df['weight']:
            if isinstance(weight, (int, float)):
                # If it's already a float or integer
                converted_weights_in_kg.append(float(weight))
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
        
        self.df['weight'] = converted_weights_in_kg
        self.df = self.df.dropna()
        return self.df

    def clean_product_data(self):
        """
        Clean and transform product data in the DataFrame.

        Returns:
            pd.DataFrame: Cleaned and transformed DataFrame containing product data.
        """
        self.df = self.convert_product_weights()
        valid_category = ['toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy']
        self.df = self.df[self.df['category'].isin(valid_category)]
        valid_availability = ['Removed', 'Still_avaliable']
        self.df = self.df[self.df['removed'].isin(valid_availability)]
        self.df['date_added'] = pd.to_datetime(self.df['date_added'], errors='coerce')
        self.df = self.df.dropna()
        self.df = self.df.reset_index(drop=True)
        return self.df
    
    def clean_orders_data(self):
        """
        Clean and transform orders data in the DataFrame.

        Returns:
            pd.DataFrame: Cleaned and transformed DataFrame containing orders data.
        """
        self.df = self.df.drop('first_name', axis=1)
        self.df = self.df.drop('last_name', axis=1)
        self.df = self.df.drop('1', axis=1)
        return self.df
    
    def clean_datetime_data(self):
        """
        Clean and transform datetime data in the DataFrame.

        Returns:
            pd.DataFrame: Cleaned and transformed DataFrame containing datetime data.
        """
        valid_timestamp_format = "%H:%M:%S"
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'], format=valid_timestamp_format, errors='coerce')
        self.df['timestamp'] = self.df['timestamp'].dt.time
        self.df = self.df.dropna(subset=['timestamp'])
        self.df['year'] = pd.to_numeric(self.df['year'], errors='coerce')
        valid_time_period = ['Evening', 'Morning', 'Midday', 'Late_Hours']
        self.df = self.df[self.df['time_period'].isin(valid_time_period)]
        return self.df
        
