import json

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

import decimal


logger = Logger()


# デバイス情報取得(契約状態:使用不可以外)
def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    )
    return response


# 接点出力制御応答取得
def get_remote_control_info(device_req_no, remote_controls_table):
    remote_control_res = remote_controls_table.query(
        KeyConditionExpression=Key("device_req_no").eq(device_req_no),
        ScanIndexForward=False,
        Limit=1,
    )
    if not "Items" in remote_control_res:
        return None
    return remote_control_res["Items"][0]
