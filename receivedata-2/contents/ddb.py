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


# ICCID管理情報取得
def get_iccid_info(sim_id, iccid_table):
    iccid_list = iccid_table.query(KeyConditionExpression=Key("iccid").eq(sim_id)).get("Items")
    return iccid_list[0] if iccid_list else None


# デバイス情報取得
def get_device_info(device_id, device_table):
    device_list = device_table.query(KeyConditionExpression=Key("device_id").eq(device_id)).get(
        "Items"
    )
    return device_list[0] if device_list else None


# 現状態取得
def get_device_state(device_id, device_state_table):
    device_state = device_state_table.query(
        KeyConditionExpression=Key("device_id").eq(device_id)
    ).get("Items")
    return device_state[0] if device_state else None


# 接点出力制御応答取得
def get_remote_control_info(device_req_no, remote_control_table):
    remote_control_info = remote_control_table.query(
        KeyConditionExpression=Key("device_req_no").eq(device_req_no)
    ).get("Items")
    return remote_control_info[0] if remote_control_info else None


# 接点出力制御応答デバイスIDキー取得
def get_remote_control_info_by_device_id(device_id, req_datetime, remote_control_table):
    start_req_datetime = req_datetime - 30000
    remote_control_info = remote_control_table.query(
        IndexName="device_id_index",
        KeyConditionExpression=Key("device_id").eq(device_id)
        & Key("event_datetime").between(start_req_datetime, req_datetime),
    ).get("Items")
    return remote_control_info[0] if remote_control_info else None


# 履歴データ挿入
def put_cnt_hist(db_item, hist_table):
    logger.debug("put_cnt_hist開始")
    item = json.loads(json.dumps(db_item), parse_float=decimal.Decimal)
    try:
        hist_table.put_item(Item=item)
        logger.debug("put_cnt_hist正常終了")
    except ClientError as e:
        logger.debug(f"put_itemエラー e={e}")


# 履歴一覧データ挿入
def put_cnt_hist_list(db_items, hist_list_table):
    logger.debug("put_cnt_hist_list")
    for db_item in db_items:
        item = json.loads(json.dumps(db_item, default=decimal_to_num), parse_float=decimal.Decimal)
        try:
            hist_list_table.put_item(Item=item)
            logger.debug("put_cnt_hist_list正常終了")
        except ClientError as e:
            logger.debug(f"put_itemエラー e={e}")


# 現状態データ更新
def update_current_state(db_item, state_table):
    item = json.loads(json.dumps(db_item, default=decimal_to_num), parse_float=decimal.Decimal)
    logger.debug(f"update_current_state item={item}")
    try:
        state_table.put_item(Item=item)
        logger.debug("update_current_state正常終了")
    except ClientError as e:
        logger.debug(f"put_itemエラー e={e}")


# 接点出力制御応答データ更新
def update_control_res(db_item, remote_control_table):
    res = remote_control_table.query(
        KeyConditionExpression=Key("device_req_no").eq(db_item["device_req_no"]),
        ScanIndexForward=False,
        Limit=1,
    )
    for cnt_state in res["Items"]:
        req_datetime = cnt_state["req_datetime"]

    logger.debug(f"req_datetime={req_datetime}")
    option = {
        "Key": {
            "device_req_no": db_item["device_req_no"],
            "req_datetime": req_datetime,
        },
        "UpdateExpression": "set #event_datetime = :event_datetime, \
                            #recv_datetime = :recv_datetime, #device_type = :device_type, \
                            #fw_version = :fw_version, #power_voltage = :power_voltage, \
                            #rssi = :rssi, #sinr = :sinr, #control_result = :control_result, \
                            #device_state = :device_state, #do_state = :do_state, #iccid = :iccid",
        "ExpressionAttributeNames": {
            "#event_datetime": "event_datetime",
            "#recv_datetime": "recv_datetime",
            "#device_type": "device_type",
            "#fw_version": "fw_version",
            "#power_voltage": "power_voltage",
            "#rssi": "rssi",
            "#sinr": "sinr",
            "#control_result": "control_result",
            "#device_state": "device_state",
            "#do_state": "do_state",
            "#iccid": "iccid",
        },
        "ExpressionAttributeValues": {
            ":event_datetime": db_item["event_datetime"],
            ":recv_datetime": db_item["recv_datetime"],
            ":device_type": db_item["device_type"],
            ":fw_version": db_item["fw_version"],
            ":power_voltage": db_item["power_voltage"],
            ":rssi": db_item["rssi"],
            ":sinr": db_item["sinr"],
            ":control_result": db_item["control_result"],
            ":device_state": db_item["device_state"],
            ":do_state": db_item["do_state"],
            ":iccid": db_item["iccid"],
        },
    }
    logger.debug(f"option={option}")

    try:
        remote_control_table.update_item(**option)
        logger.debug("update_control_res正常終了")
    except ClientError as e:
        logger.debug(f"update_itemエラー e={e}")


# 履歴情報取得
def get_history_count(simid, eventtime, hist_table):
    res = hist_table.query(
        IndexName="simid_index",
        KeyConditionExpression=Key("simid").eq(simid) & Key("event_datetime").eq(eventtime),
    )
    return res["Count"]


# 通知履歴挿入
def put_notice_hist(db_item, notification_hist_table):
    item = json.loads(json.dumps(db_item), parse_float=decimal.Decimal)
    try:
        notification_hist_table.put_item(Item=item)
        logger.debug("put_notice_hist正常終了")
    except ClientError as e:
        logger.debug(f"put_itemエラー e={e}")


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
    return group_list


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
            )
            mailaddress_list.append(account_info["email_address"])
    return mailaddress_list
