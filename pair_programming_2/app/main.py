"""Function called by PubSub trigger to execute cron job tasks."""
import pandas as pd
import numpy as np
import csv
import datetime
import time
from string import Template
import logging
import cm360_api
import gcp_storage
import configparser


date = (datetime.date.today() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d")

def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    """

    # Get default date
    date =  (datetime.date.today() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d")

    # Get dates
    bucket_name = 'unhcruk-data'

    gcpObj = gcp_storage.GCP_Storage(bucket_name)    
    dates = gcpObj.get_dates(date, prefix = 'all_sites/unattributed_conversions_')

    if len(dates) == 0:
        print("All dates already updated")
    else:
        cmObj = cm360_api.CM360_Requests()

        # CM360 IDs
        config = configparser.ConfigParser()
        config.read_file(open('./credentials/dwh.cfg'))

        profile_id = config.get('CM360','PROFILE_ID')
        report_id = str(config.get('CM360','REPORT_ID'))

        # Get list of files
        response = cmObj.get_files(profile_id)

        # Download report
        directory = 'unattributed'

        for end_date in dates:
            
            # Get file_id
            try:
                file_id = [i['id'] for i in response['items'] 
                    if (i['dateRange']['endDate'] == end_date) & (i['reportId'] == report_id)][0]
            except:
                print(end_date, " does not exist")
                continue
            cmObj.download_report(directory, report_id, file_id)
            
            # Transform report and download it to temp location
            # Get file and save it again skipping the first rows
            conversions = pd.read_csv('/tmp/unattributed/Unattributed_Conversions_Dashboard.csv', skiprows=14)

            # Add rows with 0 values in case a type of conversion doesn't exist for the dat
            zero_df = pd.DataFrame(np.array([[None, end_date, 'oneoff', 0, 0, 0], 
                                            [None, end_date, 'monthly', 0, 0, 0]]), 
                                            columns=conversions.columns)
            conversions = conversions.append(zero_df)

            unattri_conv = conversions.copy(deep=True)[conversions['Site (CM360)'].isna()]\
                                      .reset_index(drop=True)

            # Group report by date so a pivot can be created
            grouped = unattri_conv.groupby(['Date', 'Donation Type (string)'])['Total Conversions', 'Total Revenue']\
                                  .sum()\
                                  .reset_index()


            # Create pivot to have donation type as columns
            unattributed = grouped.pivot(index='Date', columns='Donation Type (string)', 
                                        values=['Total Conversions', 'Total Revenue'])\
                                .droplevel(0, axis=1)\
                                .reset_index()\
                                .fillna(0)

            unattributed.columns = ['date', 'monthly_donations', 'one_off_donations', 'mohtly_revenue', 'one_off_revenue']

            # Create missing columns
            unattributed['campaign_name'] = 'unattributed'
            unattributed['spend'] = 0
            unattributed['impressions'] = 0
            unattributed['clicks'] = 0
            unattributed['site'] = 'unattributed'

            # Order columns so they have the same order as all files in unhcruk-data/all_sites 
            unattributed = unattributed[['date', 'campaign_name', 'spend', 'impressions', 'clicks', 
                                        'one_off_donations', 'one_off_revenue', 'monthly_donations', 'mohtly_revenue',  
                                        'site']]
            
            # Upload report to Storage
            file_name = 'unattributed_conversions_{0}'.format(end_date)

            bucket_name = "unhcruk-data"
            destination_blob_name = 'all_sites/{0}.csv'.format(file_name)

            gcpObj.storage_from_filename(unattributed, directory, file_name, destination_blob_name, content_type='text/csv')        

            time.sleep(1)

    try:
        current_time = datetime.datetime.utcnow()
        log_message = Template('Cloud Function was triggered on $time')
        logging.info(log_message.safe_substitute(time=current_time))

    except Exception as error:
        log_message = Template('$error').substitute(error=error)
        logging.error(log_message)

if __name__ == '__main__':
    main('data', 'context')

