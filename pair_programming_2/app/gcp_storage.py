import pandas as pd
import os
import json
import re
import csv
from google.cloud import storage


class GCP_Storage():
    def __init__(self, bucket_name):
        
        # GCP bubcket name
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='./credentials/storage-unhcruk.json'

        self.storage_client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.storage_client.bucket(self.bucket_name)
        
        
    def get_storage_data(self, source_blob_name):

        blob = self.bucket.blob(source_blob_name)
        # Download
        json_data = blob.download_as_string()
        
        return json_data


    def storage_from_string(self, json_file, file_name, destination_blob_name):

        blob = self.bucket.blob(destination_blob_name)

        # Upload
        blob.upload_from_string(data=json.dumps(json_file), content_type='application/json')
        print("File {} uploaded to {}.".format(file_name, destination_blob_name))

        return None


    def storage_from_filename(self, df, directory, file_name, destination_blob_name, content_type):

        temp_location = '/tmp/{0}/'.format(directory)

        if not os.path.exists(temp_location):
            os.makedirs(temp_location)
            
        df.to_csv(temp_location + '/' + file_name, sep=",", index=False, quotechar='"', quoting=csv.QUOTE_ALL, encoding="utf8")

        blob = self.bucket.blob(destination_blob_name)

        # Upload
        blob.upload_from_filename(temp_location + '/' + file_name, content_type)
        print("File {} uploaded to {}.".format(file_name, destination_blob_name))

        return None


    def get_dates(self, date, prefix):
        
        # Get list of blobs
        # prefix = 'all_sites/adroll_'
        # storage_client = storage.Client()
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix=prefix)
        # Get list of blobs
        list_of_blobs = [blob for blob in blobs]
        # Get csv files names from list of blobs
        json_files = [j for i in list_of_blobs for j in str(i).split(', ') if 'csv' in j]
        # Get dates from csv files names
        list_dates = [re.sub('.csv', '', re.sub(r'.*_', '', file)) for file in json_files]
        # Sort list
        list_dates.sort()
        # Create a dates range from the min date to date
        all_dates = pd.date_range(list_dates[0], date)
        all_dates_str= [i.strftime("%Y-%m-%d") for i in all_dates]
        dates = [i for i in all_dates_str if i not in list_dates]

        return dates


        
