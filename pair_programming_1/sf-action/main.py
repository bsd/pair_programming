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
        ,a.Contact__c
        ,a.Email_Contact_ID__c
        ,a.Action__c
        ,a.Name
        ,a.Internal_Name__c
        ,a.Action_Type__c
        ,a.Date_of_Action__c
        ,a.CreatedDate
        ,a.utm_campaign__c
        ,a.utm_content__c
        ,utm_medium__c
        ,a.utm_source__c
        ,a.utm_term__c
        ,a.Campaign__c
        ,a.SystemModStamp
        FROM Sb_actions_taken__c a
        WHERE a.SystemModStamp >= 2020-04-01T00:00:00Z
        """
        #The above timestamp can be set to a more recent date whenever the historical data is refreshed

    sf_data = sf.query_all(query)
    dataframe = pd.DataFrame(sf_data['records']).drop(columns='attributes')

    #pandas_gbq method

    pandas_gbq.to_gbq(dataframe,table_id,project_id=project_id,if_exists='replace')