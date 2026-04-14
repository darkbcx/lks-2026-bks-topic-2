import sys
import awswrangler as wr
import os
import boto3
from datetime import datetime, timezone

dynamodb = boto3.client("dynamodb")
LOG_TABLE_NAME = os.getenv("LOG_TABLE_NAME", "lks-2026-visit-log")
COUNT_TABLE_NAME = os.getenv("COUNT_TABLE_NAME", "lks-2026-visit-count")

bucket_name = os.getenv("S3_BUCKET_NAME", "lks-2026-log")
now = datetime.now(timezone.utc)
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")
hour = now.strftime("%H")

def lambda_handler(event, context):
    s3_path = f"s3://{bucket_name}/gz/year={year}/month={month}/day={day}/hour={hour}"
    print(f"Reading from {s3_path}")

    objects = wr.s3.list_objects(path=s3_path)
    if not objects:
        print(f"No files found at {s3_path}. Skipping.")
        return {"statusCode": 200, "body": "No data to process."}

    df = wr.s3.read_parquet(path=s3_path, columns=['cs_method', 'cs_User_Agent', 'time', 'sc_status', 'fle_status', 'cs_uri_stem', 'cs_uri_query', 'c_ip'], dataset=True)
    # df = wr.s3.read_parquet(path=s3_path, dataset=True)

    if df.empty:
        print(f"Parquet files at {s3_path} are empty. Skipping.")
        return {"statusCode": 200, "body": "No data to process."}

    # Filter out unused HTTP status code
    df = df[(df.sc_status == "200") | (df.sc_status == "304")]

    # Filter out non GET method
    df = df[df.cs_method == 'GET']

    # Remove seconds from time
    df['time'] = df.apply(lambda x: x['time'][:-3], axis = 1)

    # Group by path
    df_log = df.groupby(['cs_User_Agent', 'time', 'cs_uri_stem', 'c_ip'])['cs_uri_stem'].count().reset_index(name='count')
    df_count = df.groupby(['cs_uri_stem'])['cs_uri_stem'].count().reset_index(name='count')
    
    count_items = []
    for index, row in df_count.iterrows():
        item = {
            "request_uri": {"S": str(row["cs_uri_stem"])},
            "request_count": {"N": str(row["count"])},
        }
        count_items.append(item)

    log_items = []
    for index, row in df_log.iterrows():
        item = {
            "request_uri": {"S": str(row["cs_uri_stem"])},
            "timestamp": {"S": f"{year}-{month}-{day}T{row['time']}Z"},
            "request_ip": {"S": str(row["c_ip"])},
            "user_agent": {"S": str(row["cs_User_Agent"])},
            "request_count": {"N": str(row["count"])},
        }
        log_items.append(item)

    if len(count_items) > 0:
        upsert_counts(count_items)
    
    if len(log_items) > 0:
        batch_put(LOG_TABLE_NAME, log_items)


def upsert_counts(items):
    print(f"Upserting {len(items)} items into {COUNT_TABLE_NAME}...")
    for item in items:
        dynamodb.update_item(
            TableName=COUNT_TABLE_NAME,
            Key={
                "request_uri": item["request_uri"],
            },
            UpdateExpression="ADD request_count :count",
            ExpressionAttributeValues={
                ":count": item["request_count"],
            },
        )
        print(f"  Upserted {item['request_uri']['S']} (+{item['request_count']['N']})")
    print(f"Done writing to {COUNT_TABLE_NAME}.")


def batch_put(table_name, items):
    print(f"Putting {len(items)} items into {table_name}...")
    for i in range(0, len(items), 25):
        batch = items[i:i + 25]
        dynamodb.batch_write_item(
            RequestItems={
                table_name: [
                    {"PutRequest": {"Item": item}} for item in batch
                ]
            }
        )
        print(f"  Written batch {i // 25 + 1} ({len(batch)} items)")
    print(f"Done writing to {table_name}.")
