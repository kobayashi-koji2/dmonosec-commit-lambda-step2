import json
import uuid
import decimal
import time

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

import db

logger = Logger()


# 接点出力制御応答取得
def get_remote_control_info(device_req_no, remote_controls_table):
    remote_control_res = remote_controls_table.query(
        KeyConditionExpression=Key("device_req_no").eq(device_req_no),
        ScanIndexForward=False,
        Limit=1,
    )
    if "Items" not in remote_control_res:
        return None
    return remote_control_res["Items"][0]


# 通知履歴テーブル作成
def put_notification_hist(
    contract_id, notification_user_list, notification_datetime, notification_hist_table
):
    notification_hist_id = str(uuid.uuid4())
    notice_hist_item = {
        "notification_hist_id": notification_hist_id,
        "contract_id": contract_id,
        "notification_datetime": int(time.mktime(notification_datetime.timetuple()) * 1000)
        + int(notification_datetime.microsecond / 1000),
        "notification_user_list": notification_user_list,
    }
    item = json.loads(json.dumps(notice_hist_item), parse_float=decimal.Decimal)
    notification_hist_table.put_item(Item=item)
    return notification_hist_id


# 履歴情報テーブル取得
def get_cnt_hist(simid, event_datetime_from, event_datetime_to, cnt_hist_table):
    sortkeyExpression = Key("event_datetime").between(
        Decimal(event_datetime_from * 1000),
        Decimal(event_datetime_to * 1000 + 999),
    )

    cnt_hist = cnt_hist_table.query(
        IndexName="simid_index",
        KeyConditionExpression=Key("simid").eq(simid) & sortkeyExpression,
    )
    return cnt_hist["Items"]


# 履歴一覧テーブルにタイムアウトレコード作成
def put_hist_list(
    remote_control,
    notification_hist_id,
    control_result,
    hist_list_table,
    device_table,
    group_table,
    device_relation_table,
):
    device = db.get_device_info(remote_control["device_id"], device_table)
    terminal_name = None
    if remote_control.get("do_no"):
        for do_list in device["device_data"]["config"]["terminal_settings"]["do_list"]:
            if do_list["do_no"] == remote_control.get("do_no"):
                terminal_name = do_list["do_name"]
    link_terminal_name = None
    if remote_control.get("link_di_no"):
        for di_list in device["device_data"]["config"]["terminal_settings"]["di_list"]:
            if di_list["di_no"] == remote_control.get("link_di_no"):
                link_terminal_name = di_list["di_name"]

    group_id_list = db.get_device_relation_group_id_list(
        device["device_id"], device_relation_table
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

    hist_list_item = {
        "device_id": device.get("device_id"),
        "hist_id": str(uuid.uuid4()),
        "event_datetime": remote_control.get("req_datetime"),
        "recv_datetime": remote_control.get("req_datetime"),  # TODO 仕様確認中
        "hist_data": {
            "device_name": device.get("device_data").get("config").get("device_name"),
            "imei": device.get("imei"),
            "group_list": group_list,
            "event_type": remote_control.get("control_trigger"),
            "terminal_no": remote_control.get("do_no"),
            "terminal_name": terminal_name,
            "link_terminal_no": remote_control.get("link_di_no"),
            "link_terminal_name": link_terminal_name,
            "control_exec_user_name": remote_control.get("control_exec_user_name"),
            "control_exec_user_email_address": remote_control.get(
                "control_exec_user_email_address"
            ),
            "control_result": control_result,
            "notification_hist_id": notification_hist_id,
            "device_req_no": remote_control.get("device_req_no"),
            "timer_time": remote_control.get("timer_time"),
        },
    }
    hist_list_table.put_item(Item=hist_list_item)


# 接点出力制御結果更新
def update_remote_control_result(
    device_req_no, req_datetime, control_result, remote_controls_table
):
    remote_controls_table.update_item(
        Key={"device_req_no": device_req_no, "req_datetime": req_datetime},
        UpdateExpression="SET control_result = :new_value",
        ExpressionAttributeValues={":new_value": control_result},
    )
