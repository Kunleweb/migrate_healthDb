import boto3
import os
import awswrangler as wr
from dotenv import load_dotenv

load_dotenv()

#connection strings
access = os.getenv('ACCESS_KEY')
secret= os.getenv('SECRET_KEY')
region= os.getenv('REGION')
bucket = 'healthbridge-data-lake'

#create a boto3 session
session = boto3.Session(aws_access_key_id=access,
                        aws_secret_access_key=secret,
                        region_name=region)

s3 = session.client('s3')



#check connection 
def check_S3(s3, bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print('Connection successful')
        return True
    except Exception as e:
        print(f'connection failed:{e}')
        return False


check_S3(s3, bucket)


