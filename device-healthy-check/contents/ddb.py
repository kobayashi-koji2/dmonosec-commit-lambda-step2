import json
import boto3
import decimal
import db
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

dynamodb = boto3.resource("dynamodb")
logger = Logger()


def decimal_to_num(obj):
    if isinstance(obj, decimal.Decimal):
        return int(obj) if float(obj).is_integer() else float(obj)


# デバイス情報取得（契約IDキー）
def get_device_info_by_contract_id(contract_id, contract_table, device_table):
    logger.debug(f"get_device_info_by_contract_id開始 contract_id={contract_id}")
    contract_info = db.get_contract_info(contract_id, contract_table)
    device_id_list = contract_info.get("contract_data", []).get("device_list", [])
    device_list = []
    for device_id in device_id_list:
        device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
        if device_info:
            device_list.append(device_info)
    return db.insert_id_key_in_device_info_list(device_list)

# 現状態取得
def get_device_state(device_id, device_state_table):
    device_state = device_state_table.query(
        KeyConditionExpression=Key("device_id").eq(device_id)
    ).get("Items")
    return device_state[0] if device_state else None


# 履歴一覧データ挿入
def put_cnt_hist_list(db_items, hist_list_table):
    for db_item in db_items:
        item = json.loads(json.dumps(db_item, default=decimal_to_num), parse_float=decimal.Decimal)
        try:
            hist_list_table.put_item(Item=item)
        except ClientError as e:
            logger.debug(f"put_cnt_hist_listエラー e={e}")


# 現状態データ更新
def update_current_state(device_id, update_digit, db_item, state_table):

    # デバイスヘルシー & 接点入力未変化
    if (update_digit & 0b0011) == 0b0011:
        option = {
            "Key": {
                "device_id": device_id,
            },
            "UpdateExpression": "set #di1_healthy_state = :di1_healthy_state, \
                                #di2_healthy_state = :di2_healthy_state, \
                                #di3_healthy_state = :di3_healthy_state, \
                                #di4_healthy_state = :di4_healthy_state, \
                                #di5_healthy_state = :di5_healthy_state, \
                                #di6_healthy_state = :di6_healthy_state, \
                                #di7_healthy_state = :di7_healthy_state, \
                                #di8_healthy_state = :di8_healthy_state, \
                                #device_healthy_state = :device_healthy_state",
            "ExpressionAttributeNames": {
                "#di1_healthy_state": "di1_healthy_state",
                "#di2_healthy_state": "di2_healthy_state",
                "#di3_healthy_state": "di3_healthy_state",
                "#di4_healthy_state": "di4_healthy_state",
                "#di5_healthy_state": "di5_healthy_state",
                "#di6_healthy_state": "di6_healthy_state",
                "#di7_healthy_state": "di7_healthy_state",
                "#di8_healthy_state": "di8_healthy_state",
                "#device_healthy_state": "device_healthy_state",
            },
            "ExpressionAttributeValues": {
                ":di1_healthy_state": db_item.get("di1_healthy_state", 0),
                ":di2_healthy_state": db_item.get("di2_healthy_state", 0),
                ":di3_healthy_state": db_item.get("di3_healthy_state", 0),
                ":di4_healthy_state": db_item.get("di4_healthy_state", 0),
                ":di5_healthy_state": db_item.get("di5_healthy_state", 0),
                ":di6_healthy_state": db_item.get("di6_healthy_state", 0),
                ":di7_healthy_state": db_item.get("di7_healthy_state", 0),
                ":di8_healthy_state": db_item.get("di8_healthy_state", 0),
                ":device_healthy_state": db_item.get("device_healthy_state", 0),
            },
        }

    # デバイスヘルシー
    elif (update_digit & 0b0001) == 0b0001:
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

    # 接点入力未変化
    elif (update_digit & 0b0010) == 0b0010:
        option = {
            "Key": {
                "device_id": device_id,
            },
            "UpdateExpression": "set #di1_healthy_state = :di1_healthy_state, \
                                #di2_healthy_state = :di2_healthy_state, \
                                #di3_healthy_state = :di3_healthy_state, \
                                #di4_healthy_state = :di4_healthy_state, \
                                #di5_healthy_state = :di5_healthy_state, \
                                #di6_healthy_state = :di6_healthy_state, \
                                #di7_healthy_state = :di7_healthy_state, \
                                #di8_healthy_state = :di8_healthy_state",
            "ExpressionAttributeNames": {
                "#di1_healthy_state": "di1_healthy_state",
                "#di2_healthy_state": "di2_healthy_state",
                "#di3_healthy_state": "di3_healthy_state",
                "#di4_healthy_state": "di4_healthy_state",
                "#di5_healthy_state": "di5_healthy_state",
                "#di6_healthy_state": "di6_healthy_state",
                "#di7_healthy_state": "di7_healthy_state",
                "#di8_healthy_state": "di8_healthy_state",
            },
            "ExpressionAttributeValues": {
                ":di1_healthy_state": db_item.get("di1_healthy_state", 0),
                ":di2_healthy_state": db_item.get("di2_healthy_state", 0),
                ":di3_healthy_state": db_item.get("di3_healthy_state", 0),
                ":di4_healthy_state": db_item.get("di4_healthy_state", 0),
                ":di5_healthy_state": db_item.get("di5_healthy_state", 0),
                ":di6_healthy_state": db_item.get("di6_healthy_state", 0),
                ":di7_healthy_state": db_item.get("di7_healthy_state", 0),
                ":di8_healthy_state": db_item.get("di8_healthy_state", 0),
            },
        }
    else:
        logger.debug(f"update_digitエラー update_digit={update_digit}")
    logger.debug(f"option={option}, update_digit={update_digit}")

    try:
        state_table.update_item(**option)
    except ClientError as e:
        logger.debug(f"update_current_stateエラー e={e}")


# 通知履歴挿入
def put_notice_hist(db_item, notification_hist_table):
    item = json.loads(json.dumps(db_item), parse_float=decimal.Decimal)
    try:
        notification_hist_table.put_item(Item=item)
    except ClientError as e:
        logger.debug(f"put_notice_histエラー e={e}")


# グループ一覧取得
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
        group_list = sorted(group_list, key=lambda x:x['group_name'])
    return group_list


# 通知先メールアドレス取得
def get_notice_mailaddress(user_id_list, user_table, account_table):
    mailaddress_list = []
    for item in user_id_list:
        users_table_res = user_table.query(
            KeyConditionExpression=Key("user_id").eq(item),
        ).get("Items", [])
        for items in users_table_res:
            account_id = items["account_id"]
            account_info = account_table.query(
                KeyConditionExpression=Key("account_id").eq(account_id)
            ).get("Items", [])
            mailaddress_list.append(account_info[0]["email_address"])
    return mailaddress_list
