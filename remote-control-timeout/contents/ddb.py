import os
import json
import uuid
import decimal
import time

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from datetime import datetime
from dateutil import relativedelta

import db

logger = Logger()

NOTIFICATION_HIST_TTL = int(os.environ["NOTIFICATION_HIST_TTL"])
HIST_LIST_TTL = int(os.environ["HIST_LIST_TTL"])


# 接点出力制御応答取得
def get_remote_control_info(device_req_no, remote_controls_table):
    remote_control_res = remote_controls_table.query(
        KeyConditionExpression=Key("device_req_no").eq(device_req_no),
        ScanIndexForward=False,
        Limit=1,
    ).get("Items", [])
    return remote_control_res[0] if remote_control_res else None


# 通知履歴テーブル作成
def put_notification_hist(
    contract_id, notification_user_list, notification_datetime, notification_hist_table
):
    notification_hist_id = str(uuid.uuid4())
    now_unixtime = int(time.mktime(notification_datetime.timetuple()) * 1000) + int(
        notification_datetime.microsecond / 1000
    )
    expire_datetime = int(
        (
            notification_datetime + relativedelta.relativedelta(years=NOTIFICATION_HIST_TTL)
        ).timestamp()
    )
    notice_hist_item = {
        "notification_hist_id": notification_hist_id,
        "contract_id": contract_id,
        "notification_datetime": now_unixtime,
        "expire_datetime": expire_datetime,
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
    device_state_table,
    limit_datetime
):
    now_unixtime = int(time.time() * 1000)

    device = db.get_device_info_other_than_unavailable(remote_control["device_id"], device_table)
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
    if group_list:
        group_list = sorted(group_list, key=lambda x: x["group_name"])

    expire_datetime = int(
        (datetime.now() + relativedelta.relativedelta(years=HIST_LIST_TTL)).timestamp()
    )

    hist_list_item = {
        "device_id": device.get("device_id"),
        "hist_id": str(uuid.uuid4()),
        "event_datetime": limit_datetime,
        # "recv_datetime": "",  # TODO 仕様確認中
        "expire_datetime": expire_datetime,
        "hist_data": {
            "device_name": device.get("device_data").get("config").get("device_name"),
            "imei": device.get("imei"),
            "group_list": group_list,
            "event_type": remote_control.get("control_trigger"),
            "terminal_no": remote_control.get("do_no"),
            "terminal_name": terminal_name,
            "link_terminal_no": remote_control.get("link_di_no"),
            "link_terminal_name": link_terminal_name,
            "link_terminal_state_name": remote_control.get("link_di_state"),
            "control_exec_user_name": remote_control.get("control_exec_user_name"),
            "control_exec_user_email_address": remote_control.get(
                "control_exec_user_email_address"
            ),
            "control_result": control_result,
            "notification_hist_id": notification_hist_id,
            "device_req_no": remote_control.get("device_req_no"),
            "timer_time": remote_control.get("timer_time"),
            "do_timer_name": remote_control.get("do_timer_name")
        },
    }

    if remote_control.get("control_trigger") in [
        "timer_control",
        "on_timer_control",
        "off_timer_control",
    ]:
        hist_data = hist_list_item["hist_data"]
        hist_data["link_terminal_state_name"] = remote_control.get("link_terminal_state_name")

    elif remote_control.get("control_trigger") in [
        "automation_control",
        "on_automation_control",
        "off_automation_control",
    ]:
        hist_data = hist_list_item["hist_data"]
        hist_data["automation_trigger_device_name"] = remote_control.get(
            "automation_trigger_device_name"
        )
        hist_data["automation_trigger_imei"] = remote_control.get("automation_trigger_imei")
        hist_data["automation_trigger_event_type"] = remote_control.get(
            "automation_trigger_event_type"
        )
        hist_data["automation_trigger_terminal_no"] = remote_control.get(
            "automation_trigger_terminal_no"
        )
        hist_data["automation_trigger_event_detail_state"] = remote_control.get(
            "automation_trigger_event_detail_state"
        )
        hist_data["automation_trigger_event_detail_flag"] = remote_control.get(
            "automation_trigger_event_detail_flag"
        )
        if remote_control.get("link_di_no"):
            device_state = db.get_device_state(remote_control["device_id"], device_state_table)
            di_no = remote_control.get("link_di_no")
            di_state_key = f"di{di_no}_state"
            link_terminal_state = device_state.get(di_state_key)
            link_terminal_state_name = ""
            for di_list in device["device_data"]["config"]["terminal_settings"]["di_list"]:
                if di_list["di_no"] == remote_control.get("link_di_no"):
                    if link_terminal_state == 0:
                        link_terminal_state_name = di_list.get("di_on_name", "クローズ")
                    elif link_terminal_state == 1:
                        link_terminal_state_name = di_list.get("di_off_name", "オープン")
            hist_data["link_terminal_state_name"] = link_terminal_state_name

    hist_list_table.put_item(Item=hist_list_item)


# 接点出力制御結果更新
def update_remote_control_result_timeout(
    device_req_no, req_datetime, link_di_no, remote_controls_table
):
    if link_di_no > 0:
        remote_controls_table.update_item(
            Key={"device_req_no": device_req_no, "req_datetime": req_datetime},
            UpdateExpression="SET control_result = :control_result, link_di_result = :link_di_result",
            ExpressionAttributeValues={":control_result": "9999", ":link_di_result": "9999"},
        )
    else:
        remote_controls_table.update_item(
            Key={"device_req_no": device_req_no, "req_datetime": req_datetime},
            UpdateExpression="SET control_result = :control_result",
            ExpressionAttributeValues={":control_result": "9999"},
        )


# 接点入力状態変化通知結果更新
def update_link_di_result_timeout(device_req_no, req_datetime, remote_controls_table):
    remote_controls_table.update_item(
        Key={"device_req_no": device_req_no, "req_datetime": req_datetime},
        UpdateExpression="SET link_di_result = :new_value",
        ExpressionAttributeValues={":new_value": "9999"},
    )
