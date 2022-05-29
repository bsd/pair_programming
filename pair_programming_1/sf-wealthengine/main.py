import os

from simple_salesforce import Salesforce
import pytz
import datetime
import pandas as pd
import pandas_gbq

def sf_query(data, context):
    sf = Salesforce(username=os.environ['SALESFORCE_USERNAME']
        , password=os.environ['SALESFORCE_PASSWORD']
        , security_token=os.environ['SALESFORCE_SECURITY_TOKEN'])
    project_id = os.environ["GCP_PROJECT"]
    table_id = os.environ['TABLE_ID']


    query = """SELECT we.Id
        ,WE_FW8_NP__Contact__c
        ,Name
        ,we.WE_FW8_NP__P2G_COMBO__c
        ,we.WE_FW8_NP__P2G_Description__c
        ,we.WE_FW8_NP__P2G_SCORE2__c
        ,we.WE_FW8_NP__P2G_SCORE__c
        ,WE_FW8_NP__AssetRange__c
        ,WE_FW8_NP__AssetRating__c
        ,WE_FW8_NP__Campaign__c
        ,WE_FW8_NP__Children_Flag__c
        ,WE_FW8_NP__d_amount__c
        ,WE_FW8_NP__d_loyalty__c
        ,WE_FW8_NP__FEC_TOT__c
        ,WE_FW8_NP__Inclination_Giving__c
        ,WE_FW8_NP__inclinationAffil__c
        ,WE_FW8_NP__IncomeRange__c
        ,WE_FW8_NP__Gender__c
        ,WE_FW8_NP__GC_AGE_USED__c
        ,SystemModstamp
        FROM WE_FW8_NP__WESearchResult__c we
        """
        #The above timestamp can be set to a more recent date whenever the historical data is refreshed

    sf_data = sf.query_all(query)
    dataframe = pd.DataFrame(sf_data['records']).drop(columns='attributes')

    #pandas_gbq method

    pandas_gbq.to_gbq(dataframe,table_id,project_id=project_id,if_exists='replace')