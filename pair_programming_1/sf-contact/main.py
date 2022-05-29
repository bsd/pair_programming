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
        c.Id
        ,c.AccountId
        ,c.Added_By__c
        ,c.CreatedDate
        ,c.Record_Type_Name__c
        ,c.Contact_Type__c
        ,c.Email_Type__c
        ,c.Primary_Email__c
        ,c.Email
        ,c.Email__c
        ,c.Initial_Market_Source__c
        ,c.Initial_Market_Source_Type__c
        ,c.First_Name_Digital__c
        ,c.Last_Name_Digital__c
        ,c.Birthdate
        ,c.Birthdate_Verified__c
        ,c.First_Action_Date__c
        ,c.First_Event_Date__c
        ,c.Sync_to_Marketing_Cloud__c
        ,c.HasOptedOutOfEmail
        ,c.Of_Email_Clicks_In_Past_6_Months__c
        ,c.of_Email_Opens_in_Past_6_Months__c
        ,c.Non_Opener_Suppression__c
        ,c.Email_Status__c
        ,c.MobilePhone
        ,c.Phone
        ,c.HomePhone
        ,c.Most_Recent_P2G__c
        ,c.Email_Opt_Out_Date__c
        ,c.Do_Not_Email__c
        ,c.npsp__Do_Not_Contact__c
        ,c.SystemModStamp
        FROM Contact c
        WHERE c.SystemModStamp >= 2021-22-01T00:00:00Z

        """
        #The above timestamp can be set to a more recent date whenever the historical data is refreshed

    sf_data = sf.query_all(query)
    dataframe = pd.DataFrame(sf_data['records']).drop(columns='attributes')

    #pandas_gbq method

    pandas_gbq.to_gbq(dataframe,table_id,project_id=project_id,if_exists='append')