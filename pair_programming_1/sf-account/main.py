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


    query = """SELECT a.Id
        ,a.Name
        ,a.X18_Char_ID__c
        ,a.CreatedDate
        ,a.Date_Added__c
        ,a.npe01__SYSTEM_AccountType__c
        ,a.Category__c
        ,a.Emergency_Donor__c
        ,a.exp_Level__c
        ,a.BillingCity
        ,a.BillingState
        ,a.BillingPostalCode
        ,a.npo02__LargestAmount__c
        ,a.Largest_Gift_Date__c
        ,a.exp_Board_Member_in_Household__c
        ,a.LastModifiedDate
        ,a.SystemModStamp
        FROM Account a
        WHERE a.SystemModStamp >= 2020-10-01T00:00:00Z
        """
        #The above timestamp can be set to a more recent date whenever the historical data is refreshed

    sf_data = sf.query_all(query)
    dataframe = pd.DataFrame(sf_data['records']).drop(columns='attributes')

    #pandas_gbq method

    pandas_gbq.to_gbq(dataframe,table_id,project_id=project_id,if_exists='replace')