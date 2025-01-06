import boto3
import psycopg2
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# Configuration
POSTGRES_CONFIG = {
    "host": "your_postgres_host",
    "port": 5432,
    "dbname": "db_name",
    "user": "user_name",
    "password": "password"
}

S3_CONFIG = {
    "bucket_name": "bucket_name",
    "access_key": "aws_access_key",
    "secret_key": "aws_secret_key",
    "region_name": "your_region"
}

REDSHIFT_CONFIG = {
    "host": "redshift_host",
    "port": 5439,
    "dbname": "redshift_db",
    "user": "redshift_user",
    "password": "redshift_password",
    "table": "target_table"
}

GOOGLE_DRIVE_CREDENTIALS_FILE = "path_to_service_account.json"

# Functions
def fetch_from_postgres():
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    query = "SELECT * FROM your_table;"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def fetch_from_s3():
    s3 = boto3.client(
        's3',
        aws_access_key_id=S3_CONFIG['access_key'],
        aws_secret_access_key=S3_CONFIG['secret_key'],
        region_name=S3_CONFIG['region_name']
    )
    obj = s3.get_object(Bucket=S3_CONFIG['bucket_name'], Key='your_s3_file.csv')
    df = pd.read_csv(obj['Body'])
    return df

def fetch_from_google_drive():
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_DRIVE_CREDENTIALS_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build('drive', 'v3', credentials=credentials)
    file_id = "your_file_id"
    request = service.files().get_media(fileId=file_id)
    fh = open('drive_file.csv', 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()
    df = pd.read_csv('drive_file.csv')
    return df

def combine_and_load_to_redshift(postgres_df, s3_df, drive_df):
    # Combine DataFrames
    combined_df = pd.concat([postgres_df, s3_df, drive_df], axis=0)
    
    # Redshift Connection
    conn = psycopg2.connect(
        host=REDSHIFT_CONFIG['host'],
        port=REDSHIFT_CONFIG['port'],
        dbname=REDSHIFT_CONFIG['dbname'],
        user=REDSHIFT_CONFIG['user'],
        password=REDSHIFT_CONFIG['password']
    )
    cursor = conn.cursor()

    # Load to Redshift
    for _, row in combined_df.iterrows():
        insert_query = f"""
        INSERT INTO {REDSHIFT_CONFIG['table']} (column1, column2, column3)
        VALUES (%s, %s, %s);
        """
        cursor.execute(insert_query, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()

# Main Execution
if __name__ == "__main__":
    postgres_df = fetch_from_postgres()
    s3_df = fetch_from_s3()
    drive_df = fetch_from_google_drive()
    
    combine_and_load_to_redshift(postgres_df, s3_df, drive_df)
    print("Data combined and loaded into Redshift successfully!")
