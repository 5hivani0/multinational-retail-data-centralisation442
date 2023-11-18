import csv
import boto3

class DataExtractor():
    def __init__(self, filename):
        self.filename = filename
    
    def extract_data_from_csv(self, file_path):
        return
    

    def extract_data_from_s3_bucket(self):
        s3 = boto3.client('s3')
        return
    
    def extract_data_from_api(self):
        return



