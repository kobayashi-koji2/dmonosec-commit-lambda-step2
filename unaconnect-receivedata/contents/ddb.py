import json
import boto3
import decimal
import db
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

dynamodb = boto3.resource("dynamodb")
logger = Logger()

#デバイス取得
def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    ).get("Items", [])

    return db.insert_id_key_in_device_info_list(response)

#履歴一覧データ挿入
def put_db_item(db_item, hist_table):
    item = json.loads(json.dumps(db_item), parse_float=decimal.Decimal)
    try:
        hist_table.put_item(Item=item)
    except ClientError as e:
        logger.debug(f"put_cnt_histエラー e={e}")

#sigfox_idをキーにdevice_id取得
def get_device_id_by_sigfox_id_info(sigfox_id,sigfox_id_table):
    sigfox_id_list = sigfox_id_table.query(KeyConditionExpression=Key("sigfox_id").eq(sigfox_id)).get("Items")
    if len(sigfox_id_list)>=1:
        device_id = sigfox_id_list[0].get("device_id")
    else:
        device_id = None
    return device_id

#グループリスト取得
def get_device_group_list(device_id, device_relation_table, group_table):
    group_id_list = db.get_device_relation_group_id_list(
        device_id, device_relation_table
    )
    group_list = []
    for group_id in group_id_list:
        group_info = db.get_group_info(group_id, group_table)
        if group_info:
            group_list.append(
                {
                    "group_id": group_info["group_id"],
                    "group_name": group_info["group_data"]["config"]["group_name"],
                }
            )
    if group_list:
        group_list = sorted(group_list, key=lambda x: x["group_name"])
    return group_list

# 現状態取得
def get_device_state(device_id, device_state_table):
    device_state = device_state_table.query(
        KeyConditionExpression=Key("device_id").eq(device_id)
    ).get("Items")
    return device_state[0] if device_state else None