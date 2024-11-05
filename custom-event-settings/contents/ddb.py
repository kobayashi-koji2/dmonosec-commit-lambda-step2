from aws_lambda_powertools import Logger
import boto3
import db
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb")
logger = Logger()


def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    ).get("Items", [])
    return db.insert_id_key_in_device_info_list(response)

#　現状態取得
def get_device_state(device_id, device_state_table):
    device_state = device_state_table.query(
        KeyConditionExpression=Key("device_id").eq(device_id)
    ).get("Items")
    return device_state[0] if device_state[0] else None
