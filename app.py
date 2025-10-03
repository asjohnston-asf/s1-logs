import csv
import json
import os
import traceback
from collections import defaultdict
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


def get_user(line: list[str]) -> str:
    if 'DownloadRole' in line[5]:
        user_id = line[5].split('/')[-1]
        if '@' in user_id and (user_id.split('@')[-1].isdigit() or user_id.split('@')[-1] == ''):
            user_id = f'{user_id.split("@")[0]}@us-west-2'
        return user_id
    if '&A-userid=' in line[9]:
        return line[9].split(' ')[1].split('=')[-1]
    return line[5].split(':')[-1]


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
            'bytes': 0 if line[12] == '-' else int(line[12]),
            'user': get_user(line),
        } for line in reader if line
    ]


def process_product(log_bucket_name: str, prefix: str, output_bucket_name: str) -> None:
    keys = get_keys(log_bucket_name, prefix)
    output = defaultdict(int)
    for key in keys:
        for log_event in get_log_events(log_bucket_name, key):
            if log_event['method'] in ('REST.GET.OBJECT', 'REST.COPY.PART_GET', 'REST.COPY.OBJECT_GET') and log_event['status_code'] in ('200', '206'):
                output[f'{log_event["key"]},{log_event["date"].strftime("%Y-%m-%d")},{log_event["user"]}'] += log_event['bytes']
    if output:
        body = '\n'.join(f'{key},{value}' for key, value in output.items())
        OUTPUT_S3_CLIENT.put_object(Bucket=output_bucket_name, Key=prefix, Body=body)


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
