import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, to_timestamp

# 1. Initialize native Glue/Spark context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_bucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket = args['s3_bucket'] 
tables = ['appointments', 'centers', 'patients', 'payments', 'test_results', 'tests']

rules = {
    'appointments': {'date_col': 'appointment_date', 'pk': 'appointment_id'},
    'centers':      {'date_col': 'created_at',       'pk': 'center_id'},
    'patients':     {'date_col': 'created_at',       'pk': 'patient_id'},
    'payments':     {'date_col': 'payment_date',     'pk': 'payment_id'},
    'test_results': {'date_col': 'test_date',        'pk': 'result_id'},
    'tests':        {'date_col': None,               'pk': 'test_id'}
}

# 2. Iterate through all tables
for table_name in tables:
    print(f"Processing table: {table_name}")
    
    date_col = rules[table_name]['date_col']
    pk = rules[table_name]['pk']
    s3_landing_path = f"s3://{bucket}/landing/{table_name}/"
    
    # Read CSV natively with Spark
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_landing_path)
    except Exception as e:
        print(f" reading this {table_name}, {e}")
        continue

    # Skip if folder was empty
    if df.isEmpty():
        continue
        
    # 3. Clean Data
    if pk and pk in df.columns:
        df = df.dropna(subset=[pk])
        df = df.dropDuplicates(subset=[pk])
        
    # 4. Partitioning & Type casting
    partition_cols = []
    if date_col and date_col in df.columns:
        # Drop rows with no date
        df = df.dropna(subset=[date_col])
        # Convert string to timestamp
        df = df.withColumn(date_col, to_timestamp(col(date_col)))
        # Drop rows where parsing failed
        df = df.dropna(subset=[date_col])
        
        # Add year and month columns for partitioning
        df = df.withColumn("year", year(col(date_col))) \
               .withColumn("month", month(col(date_col)))
               
        partition_cols = ['year', 'month']
        
    # 5. Write to Processed Layer as Parquet
    processed_path = f"s3://{bucket}/processed/{table_name}/"
    
    writer = df.write.mode("append")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
        
    writer.parquet(processed_path)
    print(f"Successfully processed {table_name}")

job.commit()
