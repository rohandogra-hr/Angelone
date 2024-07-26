import pandas as pd
import boto3
from botocore.exceptions import ClientError
from datetime import date
from io import BytesIO, StringIO
import json

# Constants
BUCKET_NAME_SOURCE = "hrdatawarehouse-sftp"
BUCKET_NAME_DESTINATION = 'hrdatamart'
TODAY_DATE = date.today().strftime("%d-%b-%Y")
DATE_2_DAYS_AGO = pd.to_datetime(date.today() - pd.DateOffset(days=2))
S3_PREFIX = f"s3:/hrdatawarehouse-sftp/iLearn_Completion_Reports/I leaner completion (All column)-{TODAY_DATE}.xlsx"
SECRET_NAME = "my_aws_credentials"
REGION_NAME = "ap-south-1"
OUTPUT_OBJECT_NAME = 'L&D/L&D.csv'

# Initialize Boto3 Clients
session = boto3.session.Session()
secrets_client = session.client(service_name='secretsmanager', region_name=REGION_NAME)
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def get_aws_credentials(secret_name, region_name):

  
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return secret['AWS_ACCESS_KEY'], secret['AWS_SECRET_KEY']
    except ClientError as e:
        print(f"Failed to retrieve secrets: {e}")
        raise

def load_excel_from_s3(bucket, prefix,s3_resource):


   
    obj = s3_resource.Object(bucket, prefix).get()
    body = obj['Body'].read()
    return pd.read_excel(BytesIO(body))

def replace_dash_with_null(value):
  
    return pd.NaT if value == '-' else value

def convert_mins_to_hrs(value):
 
    return 0 if value == '-' else float(value) / 60

def clean_text(text):
  
    return text.replace(',', '').replace('\n', '').replace('\r', '') if isinstance(text, str) else text

def process_dataframe(df):
    
    df_selected_cols = df[[
        "User Id", "Module Name", "Module Type", "Started On", "Due Date", "Time Spent (mins)",
        "Last Accessed On", "Module Status", "Enrolled On", "Completed On", "Business Vertical Name", "Skill Name"
    ]]

    date_columns = ['Last Accessed On', 'Enrolled On', 'Completed On', 'Started On']
    for col in date_columns:
        df_selected_cols[col] = df_selected_cols[col].apply(replace_dash_with_null)

    df_filtered = df_selected_cols[
        (df_selected_cols['Last Accessed On'] >= DATE_2_DAYS_AGO) |
        (df_selected_cols['Enrolled On'] >= DATE_2_DAYS_AGO)
    ]

    df_filtered['Time Spent (mins)'] = df_filtered['Time Spent (mins)'].apply(convert_mins_to_hrs)
    df_filtered.rename(columns={"Time Spent (mins)": "Time Spent (hrs)"}, inplace=True)

    new_column_order = [
        'User Id', 'Module Name', 'Module Type', 'Started On', 'Due Date',
        'Time Spent (hrs)', 'Last Accessed On', 'Module Status', 'Enrolled On',
        'Completed On', 'Skill Name', 'Business Vertical Name'
    ]
    df_reordered = df_filtered[new_column_order]
    df_reordered['Is_offline'] = "No"

    return df_reordered.applymap(clean_text)

def upload_to_s3(df, bucket, object_name):
  
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
        access_key, secret_key = get_aws_credentials(SECRET_NAME, REGION_NAME)

        # Initialize S3 resource with retrieved credentials
        s3_resource = boto3.resource(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        df = load_excel_from_s3(BUCKET_NAME_SOURCE, S3_PREFIX,s3_resource)
        df_cleaned = process_dataframe(df)
        upload_to_s3(df_cleaned, BUCKET_NAME_DESTINATION, OUTPUT_OBJECT_NAME)
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
