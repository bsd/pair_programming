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


    query = """SELECT o.AccountId
        ,o.X18_Char_ID__c
        ,o.CloseDate
        ,o.General_Status__c
        ,o.StageName
        ,o.Transaction_Date_Time__c
        ,o.Amount
        ,o.Type
        ,o.Is_Recurring_Donation__c
        ,o.First_donation__c
        ,o.Market_Source__c
        ,o.Market_Source_Type__c
        ,o.Primary_Campaign_Name__c
        ,o.Primary_Campaign_Record_Type__c
        ,o.CampaignId
        ,o.Campaign_Content__c
        ,o.Campaign_Medium__c
        ,o.Campaign_name__c
        ,o.Campaign_Source__c
        ,o.Campaign_Term__c
        ,o.exp_Reporting_Category__c
        ,o.Donation_Form_Name__c
        ,o.Donation_Form_URL__c
        ,o.npsp__Tribute_Type__c
        ,o.Add_on_Donation__c
        ,o.CC_Exp_Month__c
        ,o.CC_Exp_Year__c
        ,o.Finance_Batch_Number__c
        ,o.Allocated_funds__c
        ,o.npe03__Recurring_Donation__c
        ,o.RE_Batch_Number__c
        ,o.Trylon_Batch_Number__c
        ,o.Payment_Method__c
        ,o.Emergency_Donation__c
        ,o.SystemModStamp
        FROM Opportunity o
        WHERE o.Transaction_Date_Time__c >= 2021-03-01T00:00:00Z
        AND StageName IN ('Accepted','Chargeback Pending','Closed','Closed Won','Distribution','Distribution Paid','Final Review Pending','Hard Pledge','Hard Pledge Gov','Partially Refunded','Soft Pledge DAF Payment','Soft Pledge Family Foundation Payment','Soft Pledge Stock Payment')
        """
        #The above System Modstamp can be updated to a more recent date whenever the historical tables are refreshed
    sf_data = sf.query_all(query)
    dataframe = pd.DataFrame(sf_data['records']).drop(columns='attributes')

    #pandas_gbq method
    pandas_gbq.to_gbq(dataframe,table_id,project_id=project_id,if_exists='replace')







