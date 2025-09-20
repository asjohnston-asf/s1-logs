import os

import boto3

os.environ['AWS_PROFILE'] = 'grfn'
sqs = boto3.client('sqs')
queue_url = 'https://sqs.us-east-1.amazonaws.com/406893895021/asj-logs-dev-Queue-oAM46NwYO4CV'

for ii in range(0, 24):
    sqs.send_message(QueueUrl=queue_url, MessageBody=f's3-access/grfn-content-prod/2025-09-19-{ii:02}')
