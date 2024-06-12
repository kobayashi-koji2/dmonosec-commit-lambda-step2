from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb")
logger = Logger()


# デバイス情報取得(契約状態:使用不可以外)
def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    )
    return response


# 連動制御情報取得
def get_automation_info_list(trigger_device_id, table):
    response = table.query(
        IndexName="trigger_device_id_index",
        KeyConditionExpression=Key("trigger_device_id").eq(trigger_device_id),
    ).get("Items", [])
    response = sorted(response, key=lambda x: x.get("automation_reg_datetime", 0))
    return response
