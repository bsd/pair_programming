import sys
sys.path.append('/anaconda3/lib/python3.7/site-packages') # Replace this with the place you installed facebookads using pip
sys.path.append('file:///anaconda3/lib/python3.7/site-packages/facebook_business') # same as above

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebookads import adobjects
from facebook_business.adobjects.adaccountuser import AdAccountUser as AdUser
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.business import Business
import json
import pandas as pd
import configparser


class facebook_api():

    def __init__(self, ad_account):
        
        config = configparser.ConfigParser()
        config.read_file(open('./credentials/dwh.cfg'))

        access_token = config.get('FACEBOOK','FB_TOKEN')
        FacebookAdsApi.init(access_token=access_token)


    def my_accounts(self):

        me = AdUser(fbid='me')
        my_accounts = list(me.get_ad_accounts())

        return my_accounts
        
        
    def ads_results(self, ad_account, from_date, to_date):
    
        params = {'time_increment': 1, 'time_range': {'since': from_date, 'until': to_date}, 'level': 'ad'}
        fields = ['campaign_name', 'adset_name', 'ad_name', 'impressions', 'spend']

        my_account = AdAccount(ad_account)
        insights = my_account.get_insights(params=params, fields=fields)
        
        return pd.DataFrame(insights)


    def report(self, ads_result):

        ads_result['spend'] = ads_result['spend'].astype(float)
        ads_result['impressions'] = ads_result['impressions'].astype(float)

        insights = ads_result.groupby(['date_start', 'campaign_name'])['spend', 'impressions'] \
                             .sum() \
                             .reset_index() \
                             .rename(columns={'date_start': 'date'})
        insights['site'] = 'facebook UK'
        insights['clicks'] = 0
        insights['one_off_donations'] = 0
        insights['one_off_revenue'] = 0
        insights['monthly_donations'] = 0
        insights['mohtly_revenue'] = 0


        insights = insights[['date', 'campaign_name', 'spend', 'impressions', 'clicks', 
                             'one_off_donations', 'one_off_revenue', 'monthly_donations', 'mohtly_revenue',  
                             'site']]

        return insights



