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


    query = """SELECT 
        r.Id
        ,r.CreatedDate
        ,r.Email_Contact__c
        ,r.Email_Contact_ID__c
        ,r.Name
        ,r.Development_Contact__c
        ,r.Development_Contact_ID__c
        ,r.npe03__Date_Established__c
        ,r.npe03__Last_Payment_Date__c
        ,r.npe03__Next_Payment_Date__c
        ,r.npe03__Open_Ended_Status__c
        ,r.Status__c
        ,r.Type__c
        ,r.npe03__Amount__c
        ,r.SystemModStamp
        FROM Npe03__recurring_donation__c r
        WHERE r.SystemModStamp >= 2020-04-01T00:00:00Z
        """
        #The above timestamp can be set to a more recent date whenever the historical data is refreshed

    sf_data = sf.query_all(query)
    dataframe = pd.DataFrame(sf_data['records']).drop(columns='attributes')

    #pandas_gbq method

    pandas_gbq.to_gbq(dataframe,table_id,project_id=project_id,if_exists='replace')