import os
import awswrangler as wr
import pandas as pd
from ingestion import session
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path

# Always load the project .env even when running from ./src
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

SUPABASE_URL = os.getenv("SUPABASE_URL")
if not SUPABASE_URL:
    raise ValueError("Missing required env var: SUPABASE_URL")

engine = create_engine(
    SUPABASE_URL.strip().strip("'").strip('"'),
    connect_args={"sslmode": "require", "connect_timeout": 10},
)

S3_PATH = "s3://healthbridge-data-lake/landing"

tables = [
    "appointments",
    "centers",
    "patients",
    "payments",
    "test_results",
    "tests"
]

for table in tables:
    query = f"SELECT * FROM public.{table}"

    df = pd.read_sql(query, engine)

    wr.s3.to_csv(
        df=df,
        path=f"{S3_PATH}/{table}/",   
        dataset=True,
        mode="overwrite",
        index=False,
        boto3_session=session
    )

    print(f"Uploaded {table} to S3")