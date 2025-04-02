import sys
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Initialize Glue & Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
dynamodb = boto3.resource('dynamodb')

# Get job parameters
args = getResolvedOptions(sys.argv, ['TABLE_NAME'])

dynamodb_table = args['TABLE_NAME']
table = dynamodb.Table(dynamodb_table)

# DELETE EXISTING DATA FROM DYNAMODB
def get_table_keys():
    client = boto3.client("dynamodb")
    response = client.describe_table(TableName=dynamodb_table)
    key_schema = response["Table"]["KeySchema"]
    return [key["AttributeName"] for key in key_schema]

table_keys = get_table_keys()

# Scan and delete all items in parallel
items = []
last_evaluated_key = None

while True:
    scan_params = {"TableName": dynamodb_table}
    if last_evaluated_key:
        scan_params["ExclusiveStartKey"] = last_evaluated_key
    response = table.scan(**scan_params)
    items.extend(response["Items"])
    last_evaluated_key = response.get("LastEvaluatedKey")
    if not last_evaluated_key:
        break

df_delete = spark.createDataFrame(items)

def delete_partition(partition):
    client = boto3.resource("dynamodb")
    table = client.Table(dynamodb_table)

    for row in partition:
        key_dict = {key: row[key] for key in table_keys}
        table.delete_item(Key=key_dict)

df_delete.foreachPartition(delete_partition)
print(f"Successfully deleted all data from {dynamodb_table}")
