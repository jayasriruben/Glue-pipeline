import sys
import boto3
import json
from datetime import datetime
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

# Get parameters from Glue job arguments
args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'TABLE_NAME', 'FILE_NAME'])
s3_bucket = args['S3_BUCKET']
dynamodb_table = args['TABLE_NAME']
file_name = args['FILE_NAME']

# Initialize Spark and Glue Context
spark = SparkSession.builder.appName("ExportDynamoDBToS3").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="dynamodb",
    connection_options={"dynamodb.input.tableName": dynamodb_table}
)
df = dyf.toDF()

json_data = df.toJSON().collect()  
json_array = "[" + ",".join(json_data) + "]"  

s3 = boto3.client("s3")
s3.put_object(Bucket=s3_bucket, Key=file_name, Body=json_array, ContentType="application/json")

print(f"Successfully exported {df.count()} records from {dynamodb_table} to s3://{s3_bucket}/{file_name}")
