import pandas as pd
import json
import requests
import configparser


class Adroll_Requests():
    
    def __init__(self, advertisable_eid):

        config = configparser.ConfigParser()
        config.read_file(open('./credentials/dwh.cfg'))

        email = config.get('ADROLL','AD_EMAIL')
        psw = config.get('ADROLL','AD_PSW')
        self.apikey = config.get('ADROLL','AD_KEY')
        self.baseURI = config.get('ADROLL','AD_URI')
        self.advertisable_eid = advertisable_eid
        
        self.adroll_auth = (email, psw)
        
    def getResults(self, start_date, end_date):

        resource = 'uhura/v1/deliveries/campaign'
        breakdowns = 'entity'
        currency = 'GBP'
        request_url = '{0}{1}?apikey={2}&advertisable_eid={3}&breakdowns={4}&start_date={5}&end_date={6}&currency={7}'\
                      .format(self.baseURI, resource, self.apikey, self.advertisable_eid, 
                              breakdowns, start_date, end_date, currency)
        results = requests.get(request_url, auth=self.adroll_auth).json()

        return results
    
    
    def transformResults(self, json):
        
        results_rename = {'entity': 'campaign_eid'}
        ads_results = pd.DataFrame(json['results']['entity']).rename(columns=results_rename)
        
        return ads_results
    
        
    def getInfo(self, resource):

        request_url = '{0}{1}?apikey={2}&advertisable={3}'.format(self.baseURI, 
                                                                  resource, 
                                                                  self.apikey, 
                                                                  self.advertisable_eid)
        info = requests.get(request_url, auth=self.adroll_auth).json()
        
        return info

    
    def transformAdgroups(self, json):
    
        adgroups_df = pd.DataFrame(json['results'])[['name', 'eid', 'campaign', 'ads']]
        adgroups_df = adgroups_df.explode('ads')
        adgroups_df['ads'] = adgroups_df['ads'].apply(pd.Series)['id'].reset_index(drop=True)

        adgroups_rename = {'name': 'adgroup_name', 'eid': 'adgroup_eid', 'campaign': 'campaign_eid', 'ads': 'ad_eid'}
        adgroups_df = adgroups_df.drop_duplicates().rename(columns=adgroups_rename)

        return adgroups_df

                                       
    def transformCampaigns(self, json):
                        
        campaigns_df = pd.DataFrame(json['results'])[['name', 'eid']]

        campaigns_rename = {'name': 'campaign_name', 'eid': 'campaign_eid'}
        campaigns_df = campaigns_df.rename(columns=campaigns_rename)
        
        return campaigns_df
    
    
    def transformAds(self, json):
        
        ads_df = pd.DataFrame(json)
        ads_rename = {'name': 'ad_name', 'eid': 'ad_eid'}
        ads_df = ads_df['results'].apply(pd.Series)[['name', 'eid']].rename(columns=ads_rename)
        
        return ads_df
    
    
    def report(self, results_json, campaigns_json, start_date):
        
        campaigns_df = self.transformCampaigns(campaigns_json)        
        
        results_df = self.transformResults(results_json)
        
        ads_results = results_df.merge(campaigns_df, how='left', on='campaign_eid')\
                                .rename(columns={'cost': 'spend'})

        ads_results['site'] = 'Adroll'
        ads_results['date'] = start_date
        ads_results['impressions'] = 0
        ads_results['clicks'] = 0
        ads_results['one_off_donations'] = 0
        ads_results['one_off_revenue'] = 0
        ads_results['monthly_donations'] = 0
        ads_results['mohtly_revenue'] = 0

        ads_results = ads_results[['date', 'campaign_name', 'spend', 'impressions', 'clicks', 
                                   'one_off_donations', 'one_off_revenue', 'monthly_donations', 'mohtly_revenue',  
                                   'site']]

        return ads_results
