
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from botocore.exceptions import ClientError
import json
import time
from datetime import date
import boto3
import os
from io import BytesIO, StringIO


SECRET_NAME = "my_aws_credentials"
REGION_NAME = "ap-south-1"
BUCKET_NAME_DESTINATION = 'hrdatamart'
OUTPUT_OBJECT_NAME = 'Qualtrics/onboarding_survey.csv'
session = boto3.session.Session()
secrets_client = session.client(service_name='secretsmanager', region_name=REGION_NAME)


url_generatefile= "https://syd1.qualtrics.com/API/v3/surveys/SV_6tDFACRCwJRXQrA/export-responses"
token = "QPtajbZ19islEEazZmU44NIGdUkMIILfsv6z76Iv"

payload_1 = json.dumps({
  "format": "json",
  "compress": "false"
})
payload_2 = {}

header_1 = {
  'X-API-TOKEN': token,
  'Content-Type': 'application/json'
}

header_2 = {
  'X-API-TOKEN': token
}


def get_aws_credentials(secret_name, region_name):

    """Retrieve AWS credentials from AWS Secrets Manager."""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return secret['AWS_ACCESS_KEY'], secret['AWS_SECRET_KEY']
    except ClientError as e:
        print(f"Failed to retrieve secrets: {e}")
        raise


def get_api_data():
      
    response = requests.request("POST", url_generatefile, headers=header_1, data=payload_1)
    time.sleep(5)
    formatted =  response.json() if response and response.status_code == 200 else None
    if formatted and 'result' in formatted:
      status = formatted['result']['progressId']


    url_download_status = "https://syd1.qualtrics.com/API/v3/surveys/SV_6tDFACRCwJRXQrA/export-responses/"+status

    responseStatus = requests.get( url=url_download_status, headers=header_2, data=payload_2)
    time.sleep(5)


    formatedresponseforfileid =  responseStatus.json() if responseStatus and responseStatus.status_code == 200 else None
    if formatedresponseforfileid and 'result' in formatted:
      fileId = formatedresponseforfileid['result']['fileId']
      
    url_file_response = "https://syd1.qualtrics.com/API/v3/surveys/SV_6tDFACRCwJRXQrA/export-responses/"+fileId+"/file"

    responseOutput = requests.get(url= url_file_response, headers=header_2, data=payload_2)
    data = responseOutput.json()
    return data

def data_transformation(api_data):
    response_list=[]
    for i in range( len(api_data['responses'])):
      
        response = api_data['responses'][i]['values']
        response_list.append({key:value for key,value in response.items() })

    df = pd.DataFrame(response_list)

    df = df[['UniqueIdentifier','startDate','endDate','progress','duration','finished','recordedDate','_recordId','QID16','QID5','QID8','QID13','QID14','QID19_9','QID19_10','QID19_12','QID19_13','QID19_16','QID22_TEXT','QID6_TEXT','QID23_TEXT','QID12_TEXT','QID20_TEXT']]
    df = df.replace(',', ' ', regex=True) 
    df = df.applymap(lambda x: x.replace('\n', ' ') if isinstance(x, str) else x)


    df.insert(17,'QID19_15',pd.NA)
    df['startDate']=pd.to_datetime(df['startDate'])
    df['startDate'] = df['startDate'].dt.date
    df = df[df['startDate']>date(2024,8,15)]
    return df


def upload_to_s3(df, bucket, object_name,s3_resource):
    """Upload the DataFrame as a CSV file to S3."""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    try:
        s3_resource.Object(bucket, object_name).put(Body=csv_buffer.getvalue())
        print("File uploaded successfully.")
    except ClientError as e:
        print(f"File upload failed: {e}")
        raise

def main():
    try:
        access_key,secret_key = get_aws_credentials(SECRET_NAME,REGION_NAME)
        api_data = get_api_data()
        s3_resource = boto3.resource(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        df = data_transformation(api_data)
        upload_to_s3(df,BUCKET_NAME_DESTINATION,OUTPUT_OBJECT_NAME,s3_resource)
    except Exception as e:
        print(f"An error occured: {e}")

if __name__=='__main__':
    main()



