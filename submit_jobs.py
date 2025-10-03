import os
from datetime import datetime, timedelta
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor

import boto3

os.environ['AWS_PROFILE'] = 'edc-sandbox'
queue_url = 'https://sqs.us-west-2.amazonaws.com/011491233277/asj-s1-logs-Queue-SoOwb8HCFSYD'

log_prefixes = [
    'asf-ngap2w-p-s1-xml',
    'asf-ngap2w-p-s1-grd',
    'asf-ngap2w-p-s1-ocn',
    'asf-ngap2w-p-s1-slc',
]

current = datetime(2024, 4, 1)
end = datetime(2025, 10, 1)
interval = timedelta(minutes=10)


def chunks(lst, n=10):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def submit_batch(batch: list[str]) -> None:
    sqs = boto3.client('sqs')
    for chunk in chunks(batch, 10):
        messages = [{'Id': str(uuid4()), 'MessageBody': prefix} for prefix in chunk]
        sqs.send_message_batch(QueueUrl=queue_url, Entries=messages)


prefixes = []
while current < end:
    for log_prefix in log_prefixes:
        prefixes.append(f'{log_prefix}{current.strftime("%Y-%m-%d-%H-%M")[:-1]}')
    current += interval

with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(submit_batch, chunks(prefixes, 1000))
