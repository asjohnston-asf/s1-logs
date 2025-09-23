import os
import traceback
from datetime import datetime

import boto3

S3_CLIENT = boto3.client('s3')


def get_keys(bucket: str, prefix: str) -> list[dict]:
    paginator = S3_CLIENT.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    return [item['Key'] for page in page_iterator for item in page.get('Contents', [])]


def process_product(log_bucket_name: str, prefix: str, output_bucket_name: str) -> None:
    keys = get_keys(log_bucket_name, prefix)
    output = set()
    for key in keys:
        response = S3_CLIENT.get_object(Bucket=log_bucket_name, Key=key)
        log_content = response['Body'].read().decode('utf-8')
        for line in log_content.splitlines():
            tokens = line.split()
            method = tokens[7]
            status_code = tokens[12]
            if method in ('REST.GET.OBJECT', 'REST.COPY.PART_GET', 'REST.COPY.OBJECT_GET') and status_code in ('200', '206'):
                key = tokens[10]
                date = datetime.strptime(tokens[2][1:12], '%d/%b/%Y')
                output.add(f'{key},{date.strftime("%Y-%m-%d")}\n')
    S3_CLIENT.put_object(Bucket=output_bucket_name, Key=prefix, Body=''.join(output))


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
