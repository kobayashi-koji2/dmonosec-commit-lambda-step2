import json

import boto3
from boto3.dynamodb.conditions import Key
import decimal


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
