import os

import boto3
from aws_lambda_powertools import Logger

logger = Logger()

region_name = os.environ.get("AWS_REGION")
from_address = os.environ.get("MAIL_FROM_ADDRESS")
endpoint_url = os.environ.get("endpoint_url")


def send_email(to_address_list, subject, body):
    client = boto3.client("ses", region_name=region_name, endpoint_url=endpoint_url)

    for to_address in to_address_list:
        client.send_email(
            Source=from_address,
            Destination={"ToAddresses": [to_address]},
            Message={
                "Subject": {
                    "Data": subject,
                },
                "Body": {
                    "Text": {
                        "Data": body,
                    },
                },
            },
        )
