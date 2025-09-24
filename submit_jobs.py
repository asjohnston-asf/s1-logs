import os
from datetime import datetime, timedelta
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor

import boto3

os.environ['AWS_PROFILE'] = 'grfn'
queue_url = 'https://sqs.us-east-1.amazonaws.com/406893895021/asj-logs-dev-Queue-oAM46NwYO4CV'


def chunks(lst, n=10):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def submit_batch(batch: list[str]) -> None:
    sqs = boto3.client('sqs')
    for chunk in chunks(batch, 10):
        messages = [{'Id': str(uuid4()), 'MessageBody': prefix} for prefix in chunk]
        sqs.send_message_batch(QueueUrl=queue_url, Entries=messages)


interval = timedelta(minutes=1)
current = datetime(2025, 6, 1)
end = datetime(2025, 9, 1)

prefixes = []
while current < end:
    prefixes.append(f's3-access/grfn-content-prod/{current.strftime("%Y-%m-%d-%H-%M")}')
    current += interval

with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(submit_batch, chunks(prefixes, 1000))
