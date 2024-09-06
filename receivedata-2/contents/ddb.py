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
    iccid_list = iccid_table.query(KeyConditionExpression=Key("iccid").eq(sim_id)).get(
        "Items"
    )
    return iccid_list[0] if iccid_list else None


# デバイス情報取得
def get_device_info(device_id, device_table):
    device_list = device_table.query(
        KeyConditionExpression=Key("device_id").eq(device_id)
    ).get("Items")
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
def get_remote_control_info_by_device_id(
    device_id, recv_datetime, remote_control_table, di_trigger
):
    start_recv_datetime = recv_datetime - 20000
    logger.debug(f"device_id={device_id}, start_recv_datetime={start_recv_datetime}, recv_datetime={recv_datetime}, di_trigger={di_trigger}")
    remote_control_info = remote_control_table.query(
        IndexName="device_id_index",
        KeyConditionExpression=Key("device_id").eq(device_id)
        & Key("recv_datetime").between(start_recv_datetime, recv_datetime),
        ScanIndexForward=False,
    ).get("Items")
    logger.debug(f"remote_control_info={remote_control_info}")
    if remote_control_info:
        for item in remote_control_info:
            logger.debug(f"item={item}")
            if item["link_di_no"] == di_trigger and "link_di_result" not in item:
                return item
    return None


# 履歴データ挿入
def put_cnt_hist(db_item, hist_table):
    item = json.loads(json.dumps(db_item), parse_float=decimal.Decimal)
    try:
        hist_table.put_item(Item=item)
    except ClientError as e:
        logger.debug(f"put_cnt_histエラー e={e}")


# 履歴一覧データ挿入
def put_cnt_hist_list(db_items, hist_list_table):
    for db_item in db_items:
        item = json.loads(
            json.dumps(db_item, default=decimal_to_num), parse_float=decimal.Decimal
        )
        try:
            hist_list_table.put_item(Item=item)
        except ClientError as e:
            logger.debug(f"put_cnt_hist_listエラー e={e}")


# 現状態データ更新
def update_current_state(current_state_info, device_info, state_table):
    if device_info["device_type"] == "PJ1":
        option = {
            "Key": {
                "device_id": current_state_info['device_id'],
            },
            "UpdateExpression": "set #signal_last_update_datetime = :signal_last_update_datetime, \
                #battery_near_last_update_datetime = :battery_near_last_update_datetime, \
                #device_abnormality_last_update_datetime = :device_abnormality_last_update_datetime, \
                #parameter_abnormality_last_update_datetime = :parameter_abnormality_last_update_datetime, \
                #fw_update_abnormality_last_update_datetime = :fw_update_abnormality_last_update_datetime, \
                #di1_last_update_datetime = :di1_last_update_datetime, \
                #signal_state = :signal_state, \
                #battery_near_state = :battery_near_state, \
                #device_abnormality = :device_abnormality, \
                #parameter_abnormality = :parameter_abnormality, \
                #fw_update_abnormality = :fw_update_abnormality, \
                #di1_state = :di1_state, \
                #signal_last_change_datetime = :signal_last_change_datetime, \
                #battery_near_last_change_datetime = :battery_near_last_change_datetime, \
                #device_abnormality_last_change_datetime = :device_abnormality_last_change_datetime, \
                #parameter_abnormality_last_change_datetime = :parameter_abnormality_last_change_datetime, \
                #fw_update_abnormality_last_change_datetime = :fw_update_abnormality_last_change_datetime, \
                #di1_last_change_datetime = :di1_last_change_datetime",
            "ExpressionAttributeNames": {
                "#signal_last_update_datetime": "signal_last_update_datetime",
                "#battery_near_last_update_datetime": "battery_near_last_update_datetime",
                "#device_abnormality_last_update_datetime": "device_abnormality_last_update_datetime",
                "#parameter_abnormality_last_update_datetime": "parameter_abnormality_last_update_datetime",
                "#fw_update_abnormality_last_update_datetime": "fw_update_abnormality_last_update_datetime",
                "#di1_last_update_datetime": "di1_last_update_datetime",
                "#signal_state": "signal_state",
                "#battery_near_state": "battery_near_state",
                "#device_abnormality": "device_abnormality",
                "#parameter_abnormality": "parameter_abnormality",
                "#fw_update_abnormality": "fw_update_abnormality",
                "#di1_state": "di1_state",
                "#signal_last_change_datetime": "signal_last_change_datetime",
                "#battery_near_last_change_datetime": "battery_near_last_change_datetime",
                "#device_abnormality_last_change_datetime": "device_abnormality_last_change_datetime",
                "#parameter_abnormality_last_change_datetime": "parameter_abnormality_last_change_datetime",
                "#fw_update_abnormality_last_change_datetime": "fw_update_abnormality_last_change_datetime",
                "#di1_last_change_datetime": "di1_last_change_datetime",
            },
            "ExpressionAttributeValues": {
                ":signal_last_update_datetime": current_state_info.get("signal_last_update_datetime"),
                ":battery_near_last_update_datetime": current_state_info.get("battery_near_last_update_datetime"),
                ":device_abnormality_last_update_datetime": current_state_info.get("device_abnormality_last_update_datetime"),
                ":parameter_abnormality_last_update_datetime": current_state_info.get("parameter_abnormality_last_update_datetime"),
                ":fw_update_abnormality_last_update_datetime": current_state_info.get("fw_update_abnormality_last_update_datetime"),
                ":di1_last_update_datetime": current_state_info.get("di1_last_update_datetime"),
                ":signal_state": current_state_info.get("signal_state"),
                ":battery_near_state": current_state_info.get("battery_near_state"),
                ":device_abnormality": current_state_info.get("device_abnormality"),
                ":parameter_abnormality": current_state_info.get("parameter_abnormality"),
                ":fw_update_abnormality": current_state_info.get("fw_update_abnormality"),
                ":di1_state": current_state_info.get("di1_state"),
                ":signal_last_change_datetime": current_state_info.get("signal_last_change_datetime"),
                ":battery_near_last_change_datetime": current_state_info.get("battery_near_last_change_datetime"),
                ":device_abnormality_last_change_datetime": current_state_info.get("device_abnormality_last_change_datetime"),
                ":parameter_abnormality_last_change_datetime": current_state_info.get("parameter_abnormality_last_change_datetime"),
                ":fw_update_abnormality_last_change_datetime": current_state_info.get("fw_update_abnormality_last_change_datetime"),
                ":di1_last_change_datetime": current_state_info.get("di1_last_change_datetime"),
            },
        }
    elif device_info["device_type"] == "PJ2":
        option = {
            "Key": {
                "device_id": current_state_info['device_id'],
            },
            "UpdateExpression": "set #signal_last_update_datetime = :signal_last_update_datetime, \
                #battery_near_last_update_datetime = :battery_near_last_update_datetime, \
                #device_abnormality_last_update_datetime = :device_abnormality_last_update_datetime, \
                #parameter_abnormality_last_update_datetime = :parameter_abnormality_last_update_datetime, \
                #fw_update_abnormality_last_update_datetime = :fw_update_abnormality_last_update_datetime, \
                #di1_last_update_datetime = :di1_last_update_datetime, \
                #di2_last_update_datetime = :di2_last_update_datetime, \
                #di3_last_update_datetime = :di3_last_update_datetime, \
                #di4_last_update_datetime = :di4_last_update_datetime, \
                #di5_last_update_datetime = :di5_last_update_datetime, \
                #di6_last_update_datetime = :di6_last_update_datetime, \
                #di7_last_update_datetime = :di7_last_update_datetime, \
                #di8_last_update_datetime = :di8_last_update_datetime, \
                #do1_last_update_datetime = :do1_last_update_datetime, \
                #do2_last_update_datetime = :do2_last_update_datetime, \
                #signal_state = :signal_state, \
                #battery_near_state = :battery_near_state, \
                #device_abnormality = :device_abnormality, \
                #parameter_abnormality = :parameter_abnormality, \
                #fw_update_abnormality = :fw_update_abnormality, \
                #di1_state = :di1_state, \
                #di2_state = :di2_state, \
                #di3_state = :di3_state, \
                #di4_state = :di4_state, \
                #di5_state = :di5_state, \
                #di6_state = :di6_state, \
                #di7_state = :di7_state, \
                #di8_state = :di8_state, \
                #do1_state = :do1_state, \
                #do2_state = :do2_state, \
                #signal_last_change_datetime = :signal_last_change_datetime, \
                #battery_near_last_change_datetime = :battery_near_last_change_datetime, \
                #device_abnormality_last_change_datetime = :device_abnormality_last_change_datetime, \
                #parameter_abnormality_last_change_datetime = :parameter_abnormality_last_change_datetime, \
                #fw_update_abnormality_last_change_datetime = :fw_update_abnormality_last_change_datetime, \
                #di1_last_change_datetime = :di1_last_change_datetime, \
                #di2_last_change_datetime = :di2_last_change_datetime, \
                #di3_last_change_datetime = :di3_last_change_datetime, \
                #di4_last_change_datetime = :di4_last_change_datetime, \
                #di5_last_change_datetime = :di5_last_change_datetime, \
                #di6_last_change_datetime = :di6_last_change_datetime, \
                #di7_last_change_datetime = :di7_last_change_datetime, \
                #di8_last_change_datetime = :di8_last_change_datetime, \
                #do1_last_change_datetime = :do1_last_change_datetime, \
                #do2_last_change_datetime = :do2_last_change_datetime",
            "ExpressionAttributeNames": {
                "#signal_last_update_datetime": "signal_last_update_datetime",
                "#battery_near_last_update_datetime": "battery_near_last_update_datetime",
                "#device_abnormality_last_update_datetime": "device_abnormality_last_update_datetime",
                "#parameter_abnormality_last_update_datetime": "parameter_abnormality_last_update_datetime",
                "#fw_update_abnormality_last_update_datetime": "fw_update_abnormality_last_update_datetime",
                "#di1_last_update_datetime": "di1_last_update_datetime",
                "#di2_last_update_datetime": "di2_last_update_datetime",
                "#di3_last_update_datetime": "di3_last_update_datetime",
                "#di4_last_update_datetime": "di4_last_update_datetime",
                "#di5_last_update_datetime": "di5_last_update_datetime",
                "#di6_last_update_datetime": "di6_last_update_datetime",
                "#di7_last_update_datetime": "di7_last_update_datetime",
                "#di8_last_update_datetime": "di8_last_update_datetime",
                "#do1_last_update_datetime": "do1_last_update_datetime",
                "#do2_last_update_datetime": "do2_last_update_datetime",
                "#signal_state": "signal_state",
                "#battery_near_state": "battery_near_state",
                "#device_abnormality": "device_abnormality",
                "#parameter_abnormality": "parameter_abnormality",
                "#fw_update_abnormality": "fw_update_abnormality",
                "#di1_state": "di1_state",
                "#di2_state": "di2_state",
                "#di3_state": "di3_state",
                "#di4_state": "di4_state",
                "#di5_state": "di5_state",
                "#di6_state": "di6_state",
                "#di7_state": "di7_state",
                "#di8_state": "di8_state",
                "#do1_state": "do1_state",
                "#do2_state": "do2_state",
                "#signal_last_change_datetime": "signal_last_change_datetime",
                "#battery_near_last_change_datetime": "battery_near_last_change_datetime",
                "#device_abnormality_last_change_datetime": "device_abnormality_last_change_datetime",
                "#parameter_abnormality_last_change_datetime": "parameter_abnormality_last_change_datetime",
                "#fw_update_abnormality_last_change_datetime": "fw_update_abnormality_last_change_datetime",
                "#di1_last_change_datetime": "di1_last_change_datetime",
                "#di2_last_change_datetime": "di2_last_change_datetime",
                "#di3_last_change_datetime": "di3_last_change_datetime",
                "#di4_last_change_datetime": "di4_last_change_datetime",
                "#di5_last_change_datetime": "di5_last_change_datetime",
                "#di6_last_change_datetime": "di6_last_change_datetime",
                "#di7_last_change_datetime": "di7_last_change_datetime",
                "#di8_last_change_datetime": "di8_last_change_datetime",
                "#do1_last_change_datetime": "do1_last_change_datetime",
                "#do2_last_change_datetime": "do2_last_change_datetime",
            },
            "ExpressionAttributeValues": {
                ":signal_last_update_datetime": current_state_info.get("signal_last_update_datetime"),
                ":battery_near_last_update_datetime": current_state_info.get("battery_near_last_update_datetime"),
                ":device_abnormality_last_update_datetime": current_state_info.get("device_abnormality_last_update_datetime"),
                ":parameter_abnormality_last_update_datetime": current_state_info.get("parameter_abnormality_last_update_datetime"),
                ":fw_update_abnormality_last_update_datetime": current_state_info.get("fw_update_abnormality_last_update_datetime"),
                ":di1_last_update_datetime": current_state_info.get("di1_last_update_datetime"),
                ":di2_last_update_datetime": current_state_info.get("di2_last_update_datetime"),
                ":di3_last_update_datetime": current_state_info.get("di3_last_update_datetime"),
                ":di4_last_update_datetime": current_state_info.get("di4_last_update_datetime"),
                ":di5_last_update_datetime": current_state_info.get("di5_last_update_datetime"),
                ":di6_last_update_datetime": current_state_info.get("di6_last_update_datetime"),
                ":di7_last_update_datetime": current_state_info.get("di7_last_update_datetime"),
                ":di8_last_update_datetime": current_state_info.get("di8_last_update_datetime"),
                ":do1_last_update_datetime": current_state_info.get("do1_last_update_datetime"),
                ":do2_last_update_datetime": current_state_info.get("do2_last_update_datetime"),
                ":signal_state": current_state_info.get("signal_state"),
                ":battery_near_state": current_state_info.get("battery_near_state"),
                ":device_abnormality": current_state_info.get("device_abnormality"),
                ":parameter_abnormality": current_state_info.get("parameter_abnormality"),
                ":fw_update_abnormality": current_state_info.get("fw_update_abnormality"),
                ":di1_state": current_state_info.get("di1_state"),
                ":di2_state": current_state_info.get("di2_state"),
                ":di3_state": current_state_info.get("di3_state"),
                ":di4_state": current_state_info.get("di4_state"),
                ":di5_state": current_state_info.get("di5_state"),
                ":di6_state": current_state_info.get("di6_state"),
                ":di7_state": current_state_info.get("di7_state"),
                ":di8_state": current_state_info.get("di8_state"),
                ":do1_state": current_state_info.get("do1_state"),
                ":do2_state": current_state_info.get("do2_state"),
                ":signal_last_change_datetime": current_state_info.get("signal_last_change_datetime"),
                ":battery_near_last_change_datetime": current_state_info.get("battery_near_last_change_datetime"),
                ":device_abnormality_last_change_datetime": current_state_info.get("device_abnormality_last_change_datetime"),
                ":parameter_abnormality_last_change_datetime": current_state_info.get("parameter_abnormality_last_change_datetime"),
                ":fw_update_abnormality_last_change_datetime": current_state_info.get("fw_update_abnormality_last_change_datetime"),
                ":di1_last_change_datetime": current_state_info.get("di1_last_change_datetime"),
                ":di2_last_change_datetime": current_state_info.get("di2_last_change_datetime"),
                ":di3_last_change_datetime": current_state_info.get("di3_last_change_datetime"),
                ":di4_last_change_datetime": current_state_info.get("di4_last_change_datetime"),
                ":di5_last_change_datetime": current_state_info.get("di5_last_change_datetime"),
                ":di6_last_change_datetime": current_state_info.get("di6_last_change_datetime"),
                ":di7_last_change_datetime": current_state_info.get("di7_last_change_datetime"),
                ":di8_last_change_datetime": current_state_info.get("di8_last_change_datetime"),
                ":do1_last_change_datetime": current_state_info.get("do1_last_change_datetime"),
                ":do2_last_change_datetime": current_state_info.get("do2_last_change_datetime"),
            },
        }
    logger.debug(f"option={option}")

    try:
        state_table.update_item(**option)
    except ClientError as e:
        logger.debug(f"update_current_stateエラー e={e}")


# 接点出力制御応答データ更新
def update_control_res(db_item, remote_control_table):
    res = remote_control_table.query(
        KeyConditionExpression=Key("device_req_no").eq(db_item["device_req_no"]),
        ScanIndexForward=False,
        Limit=1,
    )
    for cnt_state in res["Items"]:
        req_datetime = cnt_state["req_datetime"]
        # 制御結果記録済みの場合は未更新
        if "control_result" in cnt_state:
            return False
        # 接点入力紐づけ有 かつ 接点入力状態変化通知結果未記録の場合は未更新
        if (
            "link_di_no" in cnt_state
            and cnt_state["link_di_no"] != 0
            and "link_di_result" not in cnt_state
        ):
            return True

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
        return True
    except ClientError as e:
        logger.debug(f"update_control_resエラー e={e}")


# 接点出力制御応答データ更新
def update_control_res_link_di_result(
    device_req_no, req_datetime, remote_control_table
):
    option = {
        "Key": {
            "device_req_no": device_req_no,
            "req_datetime": req_datetime,
        },
        "UpdateExpression": "set #link_di_result = :link_di_result",
        "ExpressionAttributeNames": {
            "#link_di_result": "link_di_result",
        },
        "ExpressionAttributeValues": {":link_di_result": "0"},
    }

    try:
        remote_control_table.update_item(**option)
    except ClientError as e:
        logger.debug(f"update_control_res_link_di_resultエラー e={e}")


# 履歴情報取得
def get_history_count(simid, eventtime, hist_table):
    res = hist_table.query(
        IndexName="simid_index",
        KeyConditionExpression=Key("simid").eq(simid)
        & Key("event_datetime").eq(eventtime),
    )
    return res["Count"]


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
        group_list = sorted(group_list, key=lambda x: x["group_name"])
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
            ).get("Items", [])
            mailaddress_list.append(account_info[0]["email_address"])
    return mailaddress_list
