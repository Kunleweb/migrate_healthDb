import boto3
import os
import awswrangler as wr
import pandas as pd
from ingestion import session
from pathlib import Path

migrate_healthDb= Path(__file__).resolve().parents[1]

datasets = {
    "appointments":migrate_healthDb/"data/appointments.csv",
    "centers": migrate_healthDb/"data/centers.csv",
    "patients":migrate_healthDb/"data/patients.csv",
    "payments": migrate_healthDb/"data/payments.csv",
    "test_results": migrate_healthDb/"data/test_results.csv",
    "tests": migrate_healthDb/"data/tests.csv"
}


for name, path in datasets.items():
    if os.path.exists(path):
        df = pd.read_csv(path, encoding='latin1')
        wr.s3.to_csv(df=df, 
                     path= f's3://healthbridge-data-lake/landing/{name}',
                     mode= 'overwrite',
                     dataset=True,
                     boto3_session=session)
    else:
        print('path does not exists')
        
print("Current working dir:", os.getcwd())