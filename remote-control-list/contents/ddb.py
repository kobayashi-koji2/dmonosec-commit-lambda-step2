from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Attr, Key

logger = Logger()


def get_device_info_only_pj2(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2) & Attr("device_type").eq("PJ2"),
    ).get("Items", [])
    return response[0] if response else None
