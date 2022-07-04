# Microsoft Authentication Library which i use to authenticate against Power BI.
import msal

# For Rest API Request
import requests

# Data returned from API call is JSON format.
import json

#to make dataframe
import pandas as pd

#Deal with dates
from datetime import date, timedelta



# Loading Snowflake connector and pandas

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd





#Set Client ID and Secret for Service Principal
client_id = "{INPUT YOUR CLIENT_ID}"

client_secret = "{INPUT YOUR CLIENT SECRET}"

authority_url = "https://login.microsoftonline.com/{ADD YOUR CLIENT_ID HERE}"


scope = ["https://analysis.windows.net/powerbi/api/.default"]


#Specify empty Dataframe with all columns
column_names = ['Id', 'RecordType', 'CreationTime', 'Operation', 'OrganizationId', 'UserType', 'UserKey', 'Workload', 'UserId', 'ClientIP', 'UserAgent', 'Activity', 'IsSuccess', 'RequestId', 'ActivityId', 'ItemName', 'WorkSpaceName', 'DatasetName', 'ReportName', 'WorkspaceId', 'ObjectId', 'DatasetId', 'ReportId', 'ReportType', 'DistributionMethod', 'ConsumptionMethod']
final_data = pd.DataFrame(columns=column_names)


activityStartDate = date.today() - timedelta(days=1)
activityStartDate = activityStartDate.strftime("%Y-%m-%d")



#Set Power BI REST API to get Activities for today
url = "https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime='" + activityStartDate + "T00:00:00'&endDateTime='" + activityStartDate + "T23:59:59'&$filter=Activity eq 'viewreport'"



#Use MSAL to grab token
app = msal.ConfidentialClientApplication(client_id, authority=authority_url, client_credential=client_secret)
result = app.acquire_token_for_client(scopes=scope)


if 'access_token' in result:
    access_token = result['access_token']
    header = {'Content-Type':'application/json', 'Authorization':f'Bearer {access_token}'}
    api_call = requests.get(url=url, headers=header)


#Set continuation URL
contUrl = api_call.json()['continuationUri']


#Get all Activities for first hour, save to dataframe and append to empty created df
result = api_call.json()['activityEventEntities']
data = pd.DataFrame(result)
pd.concat([final_data, data])


#Call Continuation URL as long as results get one back to get all activities through the day
while contUrl is not None:        
    api_call_cont = requests.get(url=contUrl, headers=header)
    contUrl = api_call_cont.json()['continuationUri']
    result = api_call_cont.json()['activityEventEntities']
    data = pd.DataFrame(result)
    final_data = pd.concat([final_data, data])

columns_list = [item.upper() for item in final_data.columns]
final_data.columns = columns_list


engine = create_engine(URL(
    account='{SNOWFLAKE ACCOUNT URL}',
    user='{USER_NAME}',
    password='{PASSWORD}',
    database='{DATABASE NAME}',
    schema='{SCHEMA NAME}',
    warehouse = '{DATA WAREHOUSE NAME}'
))



connection = engine.connect() 



# this chunck of code is just for the purpose of getting to know that connection is made. If you want to remove it in automation, you can.
# connection.execute ('Select CURRENT_VERSION()')
# row = connection.fetchone()
# print(row[0])

try:
    final_data.to_sql('{TABLE NAME WHERE YOU'LL HOST YOUR DATA}', con=connection, index=False, if_exists='append')

except Exception as e:
    print(e)


connection.close()
engine.dispose()
