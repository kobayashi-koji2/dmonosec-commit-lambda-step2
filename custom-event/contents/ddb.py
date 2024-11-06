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
        if device_info and device_info.get("device_type") in ["PJ1","PJ2","PJ3"]:
            device_list.append(device_info)
    return device_list

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


def update_current_state(device_current_state,state_table,hist_list_items):
    update_custom_timer_event_list = []
    for custom_timer_event in device_current_state.get("custom_timer_event_list"):
        for hist_list_item in hist_list_items:
            if hist_list_item.get("hist_data").get("custom_event_id") == custom_timer_event.get("custom_event_id"):
                update_di_event_list = []
                for di_event in custom_timer_event.get("di_event_list"):
                    if hist_list_item.get("hist_data").get("terminal_no") == di_event.get("di_no"):
                        di_event["event_datetime"] = 0
                        di_event["event_judge_datetime"] = 0
                    di_event["delay_flag"] = 0
                    update_di_event_list.append(di_event)                    
                custom_timer_event["di_event_list"] = update_di_event_list
        update_custom_timer_event_list.append(custom_timer_event)
    option = {
        "Key": {
            "device_id": device_current_state['device_id'],
        },
        "UpdateExpression": "#custom_timer_event_list = :custom_timer_event_list",
        "ExpressionAttributeNames": {
            "#custom_timer_event_list": "custom_timer_event_list",
        },
        "ExpressionAttributeValues": {
            ":custom_timer_event_list": update_custom_timer_event_list,
        },
    }
    logger.debug(f"option={option}")
    try:
        state_table.update_item(**option)
    except ClientError as e:
        logger.debug(f"update_current_stateエラー e={e}")