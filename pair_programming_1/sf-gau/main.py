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


    query = """SELECT Id
        ,Posted_Date__c
        ,Posting_Type__c
        ,Opportunity_Hidden__c
        ,npsp__Opportunity__c
        ,npsp__Campaign__c
        ,Name
        ,Locked_Revenue_GL_Credit_Code__c
        ,npsp__General_Accounting_Unit__c
        ,Fund_GL_Code__c
        ,SystemModstamp
        FROM Npsp__allocation__c g
        WHERE g.SystemModStamp >= 2021-03-01T00:00:00Z
        """
        #The above timestamp can be set to a more recent date whenever the historical data is refreshed

    sf_data = sf.query_all(query)
    dataframe = pd.DataFrame(sf_data['records']).drop(columns='attributes')

    #pandas_gbq method

    pandas_gbq.to_gbq(dataframe,table_id,project_id=project_id,if_exists='replace')