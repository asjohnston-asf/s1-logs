import os
from datetime import datetime, timedelta
from uuid import uuid4

import boto3

os.environ['AWS_PROFILE'] = 'grfn'
sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/406893895021/asj-logs-dev-Queue-oAM46NwYO4CV'


def chunks(lst, n=10):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


interval = timedelta(minutes=1)
current = datetime(2025, 9, 1)
end = datetime(2025, 9, 23)

prefixes = []
while current < end:
    prefixes.append(f's3-access/grfn-content-prod/{current.strftime("%Y-%m-%d-%H-%M")}')
    current += interval

ii = 0
for chunk in chunks(prefixes):
    messages = [{'Id': str(uuid4()), 'MessageBody': prefix} for prefix in chunk]
    sqs.send_message_batch(QueueUrl=queue_url, Entries=messages)
    ii += len(messages)
    print(f'{ii}/{len(prefixes)}')
