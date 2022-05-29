"""Function called by PubSub trigger to execute cron job tasks."""
import pandas as pd
import numpy as np
import datetime
import logging
from string import Template
import time
import adroll_api
import facebook_api
from io import StringIO
from calendar import monthrange
import gcp_storage
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import storage


date = (datetime.date.today() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d")

def main(data, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    """

    # Get default mainCurrency and date
    date =  (datetime.date.today() - datetime.timedelta(days = 1)).strftime("%Y-%m-%d")
    # FB's ad account
    ad_account = 'act_2010817465893182'
    fbObj = facebook_api.facebook_api(ad_account)

    # Adroll's advertisable
    advertisable_eid = 'CJG6Y263JBCNXM4UR2QNCB'
    adrObj = adroll_api.Adroll_Requests(advertisable_eid)

    # Get dates that need to be updated
    bucket_name = "unhcruk-data"
    gcpObj = gcp_storage.GCP_Storage(bucket_name)    
    dates = gcpObj.get_dates(date, prefix = 'all_sites/adroll_')
    print(dates)

    if len(dates) == 0:
        print("All dates already updated")
    else:
        # Get Zeta's data
        bucket_name = "zeta-unhcr"
        source_blob_name = 'UNHCR_SpendReport.csv'
        gcpObj = gcp_storage.GCP_Storage(bucket_name)
        b_file = gcpObj.get_storage_data(source_blob_name)

        # Create dataframe witj Zeta's data
        s=str(b_file,'utf-8')
        data = StringIO(s) 
        report=pd.read_csv(data)

        # Add the required columns
        report = report.rename(columns={'campaign': 'campaign_name'})
        report['impressions'] = 0
        report['clicks'] = 0
        report['one_off_donations'] = 0
        report['one_off_revenue'] = 0
        report['monthly_donations'] = 0
        report['mohtly_revenue'] = 0
        report['site'] = 'Zeta Global'
        report = report[['date', 'campaign_name', 'spend', 'impressions', 'clicks', 
                         'one_off_donations', 'one_off_revenue', 'monthly_donations', 'mohtly_revenue',  
                         'site']]

        # Upload Zeta's report
        bucket_name = "unhcruk-data"
        destination_blob_name = 'all_sites/zeta_global.csv'
        file_name = 'zeta_global.csv'
        directory = 'zeta'
        gcpObj = gcp_storage.GCP_Storage(bucket_name)
        gcpObj.storage_from_filename(report, directory, file_name, destination_blob_name, content_type='text/csv')


        # Update FB. Get month that needs to be updated
        d = pd.to_datetime(date).day
        m = pd.to_datetime(date).month
        y = pd.to_datetime(date).year

        prev_m = 12 if m == 1 else m - 1
        prev_m_y = y-1 if m == 1 else y
        
        from_date = '{0}-{1}-{2}'.format(y, m, '1')
        num_days = monthrange(y, m)[1]
        to_date = '{0}-{1}-{2}'.format(y, m, num_days)

        # Get FB's data
        insights_df = fbObj.ads_results(ad_account, from_date, to_date)
        insights = fbObj.report(insights_df)

        # Upload to storage
        directory = 'facebook'
        destination_blob_name = 'all_sites/facebook_{0}_{1}.csv'.format(from_date, to_date)
        file_name = 'facebook_{0}_{1}.csv'.format(from_date, to_date)
        gcpObj.storage_from_filename(insights, directory, file_name, destination_blob_name, content_type='text/csv')

        # Check if any of the dates in dates is from the previous month. If so, update previous month
        months = [pd.to_datetime(i).month for i in dates]

        # If it's day 1, 2 or 3 of this month or if any of the dates in dates is from the previous month, update previous month as well
        if (d < 5) | ((12 if m == 1 else prev_m) in months):
            from_date = '{0}-{1}-{2}'.format(prev_m_y, (12 if m == 1 else prev_m), '1')
            num_days = monthrange(prev_m_y, (12 if m == 1 else prev_m))[1]
            to_date = '{0}-{1}-{2}'.format(prev_m_y, (12 if m == 1 else prev_m), num_days)

            # Get FB's data
            insights_df = fbObj.ads_results(ad_account, from_date, to_date)
            insights = fbObj.report(insights_df)

            # Upload to storage
            destination_blob_name = 'all_sites/facebook_{0}_{1}.csv'.format(from_date, to_date)
            file_name = 'facebook_{0}_{1}.csv'.format(from_date, to_date)
            gcpObj.storage_from_filename(insights, directory, file_name, destination_blob_name, content_type='text/csv')
        else:
            print('Previous month does not need to be updated')

        # Update Adroll's data
        get_campaigns = 'api/v1/advertisable/get_campaigns'
        campaigns_json = adrObj.getInfo(get_campaigns)
        directory = 'adroll'
        
        for start_date in dates:
            end_date = str(np.datetime64(start_date, 'D') + 1)
            results_json = adrObj.getResults(start_date, end_date)
            
            if len(results_json['results']['entity']) > 0 :
                report = adrObj.report(results_json, campaigns_json, start_date)        
            
            else:
                print('no_data')
                report = pd.DataFrame(columns=['date', 'campaign_name', 'spend', 'impressions', 'clicks', 
                                               'one_off_donations', 'one_off_revenue', 'monthly_donations', 
                                               'mohtly_revenue',  'site'])
                report['date'] = start_date
                report['site'] = 'Adroll'

            destination_blob_name = 'all_sites/adroll_{0}.csv'.format(start_date)
            file_name = 'adroll_{0}.csv'.format(start_date)
            gcpObj.storage_from_filename(report, directory, file_name, destination_blob_name, content_type='text/csv')
                
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

