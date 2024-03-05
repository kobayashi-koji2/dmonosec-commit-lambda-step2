import os
import decimal
import re
import json
from operator import itemgetter

import boto3
from boto3.dynamodb.conditions import Attr, Key
from aws_lambda_powertools import Logger

import db
import ssm
import convert

logger = Logger()

AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
LAMBDA_TIMEOUT_CHECK = os.environ["LAMBDA_TIMEOUT_CHECK"]

aws_lambda = boto3.client("lambda", region_name=AWS_DEFAULT_REGION)
iot = boto3.client("iot-data", region_name=AWS_DEFAULT_REGION)
dynamodb = boto3.resource("dynamodb")
client = boto3.client(
    "dynamodb",
    region_name="ap-northeast-1",
    endpoint_url=os.environ.get("endpoint_url"),
)


def automation_control(device_id, event_type, terminal_no, di_state, occurrence_flag):

    # パラメータチェック
    if not event_type:
        return {"result": False, "message": "イベント項目が指定されていません。"}
    if event_type == "di_change":
        if not terminal_no:
            return {"result": False, "message": "接点端子が指定されていません。"}
        if not di_state:
            return {"result": False, "message": "接点入力状態が指定されていません。"}
    elif event_type == "di_healthy":
        if not terminal_no:
            return {"result": False, "message": "接点端子が指定されていません。"}
        if occurrence_flag is None:
            return {"result": False, "message": "発生フラグが指定されていません。"}
    else:
        if occurrence_flag is None:
            return {"result": False, "message": "発生フラグが指定されていません。"}

    # テーブル取得
    try:
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        device_state_table = dynamodb.Table(ssm.table_names["STATE_TABLE"])
        req_no_counter_table = dynamodb.Table(ssm.table_names["REQ_NO_COUNTER_TABLE"])
        remote_controls_table = dynamodb.Table(ssm.table_names["REMOTE_CONTROL_TABLE"])
        hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
        group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
        notification_hist_table = dynamodb.Table(ssm.table_names["NOTIFICATION_HIST_TABLE"])
        automations_table = dynamodb.Table(ssm.table_names["AUTOMATIONS_TABLE"])
    except KeyError as e:
        return {"result": False, "message": e}

    # 連動制御設定取得
    automation = _get_automation(
        automations_table, device_id, event_type, terminal_no, di_state, occurrence_flag
    )
    if not automation:
        return {"result": False, "message": "連想制御設定が存在しません。"}

    # 制御対象デバイス情報取得
    control_device = db.get_device_info_other_than_unavailable(
        automation["control_device_id"], device_table
    )

    # 制御対象デバイスの接点出力設定取得
    control_device_do_list = (
        control_device.get("device_data", {})
        .get("config", {})
        .get("terminal_settings", {})
        .get("do_list", [])
    )
    control_do = [
        do for do in control_device_do_list if do["do_no"] == automation["control_do_no"]
    ][0]

    # 制御対象デバイスの紐づけ接点入力が指定されている場合、接点入力状態をチェック
    if control_do.get("do_di_return") and automation["control_di_state"] in [0, 1]:
        device_state = db.get_device_state(automation["control_device_id"], device_state_table)
        if not device_state:
            return {"result": False, "message": "制御対象デバイスの現状態情報が存在しません。"}

        col_name = "di" + str(control_do.get("do_di_return")) + "_state"
        if device_state[col_name] == automation["control_di_state"]:
            # 紐づき接点入力状態がすでに変更済みのため、制御不要
            # TODO メール通知
            # TODO 履歴情報登録
            return {
                "result": False,
                "message": "制御対象デバイスの接点入力状態がすでに変更済みです。",
            }

    # TODO 制御中判定
    # TODO メール通知
    # TODO 履歴情報登録

    # 要求番号生成
    icc_id = control_device["device_data"]["param"]["iccid"]
    req_no = _get_req_no(icc_id, req_no_counter_table)

    # 制御実行（MQTT）
    _cmd_exec(icc_id, req_no, control_do)

    # TODO 要求データ登録
    device_req_no = icc_id + "-" + req_no

    # タイムアウト判定Lambda呼び出し
    payload = {"body": json.dumps({"device_req_no": device_req_no})}
    lambda_invoke_result = aws_lambda.invoke(
        FunctionName=LAMBDA_TIMEOUT_CHECK,
        InvocationType="Event",
        Payload=json.dumps(payload, ensure_ascii=False),
    )
    logger.info(f"lambda_invoke_result: {lambda_invoke_result}")


def _cmd_exec(icc_id, req_no, control_do):
    topic = "cmd/" + icc_id
    do_no = int(control_do["do_no"])
    do_specified_time = float(control_do["do_specified_time"])

    if control_do["do_control"] == "open":
        do_control = "01"
        # 制御時間は 0.1 秒を 1 として16進数4バイトの値を設定
        do_control_time = re.sub("^0x", "", format(int(do_specified_time * 10), "#06x"))
    elif control_do["do_control"] == "close":
        do_control = "00"
        # 制御時間は 0.1 秒を 1 として16進数4バイトの値を設定
        do_control_time = re.sub("^0x", "", format(int(do_specified_time * 10), "#06x"))
    elif control_do["do_control"] == "toggle":
        do_control = "10"
        do_control_time = "0000"

    payload = {
        "Message_Length": "000C",
        "Message_type": "8002",
        "Req_No": req_no,
        "DO_No": format(do_no, "#02"),
        "DO_Control": do_control,
        "DO_ControlTime": do_control_time,
    }
    logger.info(f"Iot Core Message: {payload}")
    pubhex = "".join(payload.values())
    logger.info(f"Iot Core Message(hexadecimal): {pubhex}")

    topic = "cmd/" + icc_id
    iot_result = iot.publish(topic=topic, qos=0, retain=False, payload=bytes.fromhex(pubhex))
    logger.info(f"iot_result: {iot_result}")


def _get_req_no(req_no_counter_table, sim_id):
    req_no_count_info = req_no_counter_table.get_item(Key={"simid": sim_id}).get("Item", {})
    if req_no_count_info:
        # 要求番号生成（アトミックカウンタをインクリメントし、要求番号を取得）
        response = req_no_counter_table.update_item(
            Key={"simid": sim_id},
            UpdateExpression="ADD #key :increment",
            ExpressionAttributeNames={"#key": "num"},
            ExpressionAttributeValues={":increment": decimal.Decimal(1)},
            ReturnValues="UPDATED_NEW",
        )
        num = response.get("Attributes").get("num")
        count = int(convert.decimal_default_proc(num))

    else:
        count = 0
        # TODO なぜトランザクションを使っているのか？要確認
        write_items = [
            {
                "Put": {
                    "TableName": req_no_counter_table.name,
                    "Item": {"simid": {"S": sim_id}, "num": {"N": str(count)}},
                }
            }
        ]
        db.execute_transact_write_item(write_items)

    return re.sub("^0x", "", format(count % 65535, "#010x"))


def _get_automation(
    automations_table, device_id, event_type, terminal_no, di_state, occurrence_flag
):
    automation_list = automations_table.query(
        IndexName="trigger_device_id_index",
        KeyConditionExpression=Key("trigger_device_id").eq(device_id),
    )["Items"]
    automation_list = [
        item
        for item in automation_list
        if item.get("trigger_event_type") == event_type
        and (
            event_type == "di_change"
            and item.get("trigger_terminal_no") == terminal_no
            and item.get("trigger_di_state") == di_state
        )
        or (
            event_type == "di_healthy"
            and item.get("trigger_terminal_no") == terminal_no
            and item.get("trigger_occurrence_flag") == occurrence_flag
        )
        or (
            event_type not in ["di_change", "di_healthy"]
            and item.get("trigger_occurrence_flag") == occurrence_flag
        )
    ]
    return automation_list[0] if automation_list else None
