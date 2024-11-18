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


def decimal_to_num(obj):
    if isinstance(obj, decimal.Decimal):
        return int(obj) if float(obj).is_integer() else float(obj)
    
#デバイス取得
def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    ).get("Items", [])

    return db.insert_id_key_in_device_info(response[0]) if response else None

#現状態データ更新
def put_db_item(db_item, table):
    item = json.loads(json.dumps(db_item,default=decimal_to_num), parse_float=decimal.Decimal)
    try:
        table.put_item(Item=item)
    except ClientError as e:
        logger.debug(f"put_db_itemエラー e={e} item = {item}")

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

# 通知履歴挿入
def put_notice_hist(db_item, notification_hist_table):
    item = json.loads(json.dumps(db_item), parse_float=decimal.Decimal)
    try:
        notification_hist_table.put_item(Item=item)
    except ClientError as e:
        logger.debug(f"put_notice_histエラー e={e}")


# 現状態デバイスヘルシー更新
def update_current_healthy_state(device_id, db_item, state_table):

    # デバイスヘルシー
    option = {
        "Key": {
            "device_id": device_id,
        },
        "UpdateExpression": "set #device_healthy_state = :device_healthy_state",
        "ExpressionAttributeNames": {
            "#device_healthy_state": "device_healthy_state",
        },
        "ExpressionAttributeValues": {
            ":device_healthy_state": db_item.get("device_healthy_state", 0),
        },
    }

    logger.debug(f"option={option}")

    try:
        state_table.update_item(**option)
    except ClientError as e:
        logger.debug(f"update_current_stateエラー e={e}")
