import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame

# Get parameters from Glue job arguments
args = getResolvedOptions(sys.argv, ['S3_BUCKET', 'S3_KEY', 'TABLE_NAME'])
s3_bucket = args['S3_BUCKET']
s3_key = args['S3_KEY']
table_name = args['TABLE_NAME']

# Initialize Spark and Glue context
spark = SparkSession.builder.appName("ImportS3ToDynamoDB").getOrCreate()
glueContext = GlueContext(spark.sparkContext)
s3_path = f"s3://{s3_bucket}/{s3_key}"
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(table_name)

# Delete all existing data from DynamoDB
def delete_all_items():
    response = dynamodb.meta.client.describe_table(TableName=table_name)
    key_schema = response['Table']['KeySchema']
    keys = [key['AttributeName'] for key in key_schema]

    # Scan and delete all items
    scan = table.scan(ProjectionExpression=", ".join(keys))
    with table.batch_writer() as batch:
        for item in scan['Items']:
            key_dict = {key: item[key] for key in keys}
            batch.delete_item(Key=key_dict)

    print(f"Deleted all items from {table_name}")

delete_all_items()

# Read data from S3 (Assuming JSON format)
df = spark.read.json(s3_path)
dynamic_frame = DynamicFrame.fromDF(df, glueContext)

# Write to DynamoDB
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="dynamodb",
    connection_options={"dynamodb.output.tableName": table_name}
)

print(f"Imported data from {s3_path} to {table_name}") 
