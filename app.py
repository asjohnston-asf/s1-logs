import csv
import json
import os
import traceback
from datetime import datetime

import boto3


def get_credentials() -> dict[str, str]:
    secretsmanager = boto3.client('secretsmanager')
    response = secretsmanager.get_secret_value(SecretId=os.environ['SECRET_ARN'])
    return json.loads(response['SecretString'])


OUTPUT_S3_CLIENT = boto3.client('s3')
INPUT_S3_CLIENT = boto3.client('s3', **get_credentials())


def get_keys(bucket: str, prefix: str) -> list[str]:
    paginator = INPUT_S3_CLIENT.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    return [item['Key'] for page in page_iterator for item in page.get('Contents', [])]


def get_log_events(bucket: str, key: str) -> list[dict]:
    response = INPUT_S3_CLIENT.get_object(Bucket=bucket, Key=key)
    log_content = response['Body'].read().decode('utf-8')
    reader = csv.reader(log_content.splitlines(), delimiter=' ')
    return [
        {
            'method': line[7],
            'status_code': line[10],
            'key': line[8],
            'date': datetime.strptime(line[2][1:12], '%d/%b/%Y'),
        } for line in reader if line
    ]


def process_product(log_bucket_name: str, prefix: str, output_bucket_name: str) -> None:
    keys = get_keys(log_bucket_name, prefix)
    output = set()
    for key in keys:
        for log_event in get_log_events(log_bucket_name, key):
            if log_event['method'] in ('REST.GET.OBJECT', 'REST.COPY.PART_GET', 'REST.COPY.OBJECT_GET') and log_event['status_code'] in ('200', '206'):
                output.add(f'{log_event["key"]},{log_event["date"].strftime("%Y-%m-%d")}\n')
    OUTPUT_S3_CLIENT.put_object(Bucket=output_bucket_name, Key=prefix, Body=''.join(output))


def lambda_handler(event: dict, _) -> dict:
    log_bucket_name = os.environ['LOG_BUCKET_NAME']
    output_bucket_name = os.environ['BUCKET_NAME']

    batch_item_failures = []
    for record in event['Records']:
        try:
            prefix = record['body']
            process_product(log_bucket_name, prefix, output_bucket_name)
        except Exception:
            print(traceback.format_exc())
            print(f'Could not process message {record["messageId"]}')
            batch_item_failures.append({'itemIdentifier': record['messageId']})
    return {'batchItemFailures': batch_item_failures}
