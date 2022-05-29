import os
import os.path
import requests
import io
import httplib2
from googleapiclient import discovery
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from googleapiclient import http
from oauth2client.file import Storage
from oauth2client import client, tools
from oauth2client.tools import run_flow, argparser

from google.cloud import storage
import pickle

class CM360_Requests():
    def __init__(self):

        # If modifying these scopes, delete the file token.json.
        SCOPES = ['https://www.googleapis.com/auth/dfareporting', 
                  'https://www.googleapis.com/auth/dfatrafficking', 
                  'https://www.googleapis.com/auth/ddmconversions']

        # # Set up a Flow object to be used if we need to authenticate.
        # flow = client.flow_from_clientsecrets('./credentials/client_secret.json', scope=SCOPES)

        # # Check whether credentials exist in the credential store. Using a credential
        # # store allows auth credentials to be cached, so they survive multiple runs
        # # of the application. This avoids prompting the user for authorization every
        # # time the access token expires, by remembering the refresh token.
        # storage = Storage('token.json')
        # credentials = storage.get()

        # # If no credentials were found, go through the authorization process and
        # # persist credentials to the credential store.
        # if credentials is None or credentials.invalid:
        #     credentials = tools.run_flow(flow, storage, tools.argparser.parse_known_args()[0])

        # # Use the credentials to authorize an httplib2.Http instance.
        # http = credentials.authorize(httplib2.Http())

        # # Construct a service object via the discovery service.
        # self.service = discovery.build('dfareporting', 'v3.4', http=http)

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='./credentials/storage-unhcruk.json'

        storage_client = storage.Client()
        bucket_name = 'unhcruk'
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob('credentials.pickle')
        with open("/tmp/my-secure-file", "wb") as file_obj:
            blob.download_to_file(file_obj)
        with open("/tmp/my-secure-file", "rb") as file_obj:
            creds = pickle.load(file_obj)
        
        # creds = service_account.Credentials.from_service_account_file('./credentials/storage-unhcruk.json', scopes=SCOPES)

        self.service = discovery.build('dfareporting', 'v3.4', credentials=creds)
        
        
    def get_files(self, profile_id):
        """Gets list of available files"""
        
        request = self.service.files().list(profileId=profile_id, sortField='LAST_MODIFIED_TIME')
        response = request.execute()
        
        response_copy = dict(response)
        while response_copy['nextPageToken']:
            request = self.service.reports().files().list_next(request, response_copy)
            response_copy = request.execute()
            response['items'] += response_copy['items']

        return response

        
    def download_report(self, directory, report_id, file_id):
        """Downloads report"""

        def generate_file_name(report_file):
            """Generates a report file name based on the file metadata."""
            # If no filename is specified, use the file ID instead.
            file_name = report_file['fileName'] or report_file['id']
            extension = '.csv' if report_file['format'] == 'CSV' else '.xml'
            return file_name + extension

        # Temp location
        temp_location = '/tmp/{0}/'.format(directory)

        if not os.path.exists(temp_location):
            os.makedirs(temp_location)

        report_file = self.service.files().get(reportId=report_id, fileId=file_id).execute()
        
        # Prepare a local file to download the report contents to.
        out_file = io.FileIO(temp_location + generate_file_name(report_file), mode='wb')

        # Create a get request.
        request = self.service.files().get_media(reportId=report_id, fileId=file_id)

        # Create a media downloader instance.
        # Optional: adjust the chunk size used when downloading the file.
        downloader = http.MediaIoBaseDownload(out_file, request)

        # Execute the get request and download the file.
        download_finished = False
        while download_finished is False:
            _, download_finished = downloader.next_chunk()

        print('File %s downloaded to %s' % (report_file['id'], os.path.realpath(out_file.name)))
        
        return None


