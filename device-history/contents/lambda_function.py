import json
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

import auth
import convert
import ssm
import ddb
import validate

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


CONTROL_NAME_DICT = {
    "open": "開制御",
    "close": "閉制御",
    "toggle": "トグル",
}


def create_history_message(hist):
    msg = ""
    # 接点入力変化
    if hist["event_type"] == "di_change":
        terminal_name = hist.get("terminal_name", "接点入力" + str(hist.get("terminal_no", "")))
        msg = f"【接点入力変化】\n{terminal_name}が{hist['terminal_state_name']}に変化しました。"

    # 接点出力変化
    elif hist["event_type"] == "do_change":
        terminal_name = hist.get("terminal_name", "接点出力" + str(hist.get("terminal_no", "")))
        msg = f"【接点出力変化（{CONTROL_NAME_DICT.get(hist['control'])}）】\n{terminal_name}が{hist['terminal_state_name']}に変化しました。"

    # アナログ入力変化（Ph2）
    elif hist["event_type"] == "ai_change":
        msg = ""

    # バッテリーニアエンド
    elif hist["event_type"] == "battery_near":
        if hist["occurrence_flag"] == 1:
            msg = "【電池残量変化（少ない）】\nデバイスの電池残量が少ない状態に変化しました。"
        elif hist["occurrence_flag"] == 0:
            msg = "【電池残量変化（十分）】\nデバイスの電池残量が十分な状態に変化しました。"

    # 機器異常
    elif hist["event_type"] == "device_abnormality":
        if hist["occurrence_flag"] == 1:
            msg = "【機器異常（発生）】\n機器異常が発生しました。"
        elif hist["occurrence_flag"] == 0:
            msg = "【機器異常（復旧）】\n機器異常が復旧しました。"

    # パラメータ異常
    elif hist["event_type"] == "parameter_abnormality":
        if hist["occurrence_flag"] == 1:
            msg = "【パラメータ異常（発生）】\nパラメータ異常が発生しました。"
        elif hist["occurrence_flag"] == 0:
            msg = "【パラメータ異常（復旧）】\nパラメータ異常が復旧しました。"

    # FW更新異常
    elif hist["event_type"] == "fw_update_abnormality":
        if hist["occurrence_flag"] == 1:
            msg = "【FW更新異常（発生）】\nFW更新異常が発生しました。"
        elif hist["occurrence_flag"] == 0:
            msg = "【FW更新異常（復旧）】\nFW更新異常が復旧しました。"

    # 電源ON
    elif hist["event_type"] == "power_on":
        msg = "【電源ON】\nデバイスの電源がONになりました。"

    # デバイスヘルシー未受信（Ph2）
    elif hist["event_type"] == "device_unhealthy":
        msg = ""

    # 接点入力未変化検出（Ph2）
    elif hist["event_type"] == "di_unhealthy":
        msg = ""

    # 画面操作による制御
    elif hist["event_type"] == "manual_control":
        terminal_name = hist.get("terminal_name", "接点出力" + str(hist.get("terminal_no", "")))
        control_exec_uer_name = (
            hist.get("control_exec_user_name")
            if hist.get("control_exec_user_name")
            else hist.get("control_exec_user_email_address")
        )
        if not hist.get("link_terminal_no"):
            if hist["control_result"] == "success" or hist["control_result"] == "failure":
                msg = f"【画面操作による制御（成功）】\n{terminal_name}の制御信号がデバイスに届きました。\n※{control_exec_uer_name}が操作を行いました。"
            elif hist["control_result"] == "timeout_response":
                msg = f"【画面操作による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きませんでした。\n※{control_exec_uer_name}が操作を行いました。"
            elif hist["control_result"] == "not_excuted_done":
                msg = f"【画面操作による制御（不実施）】\n他のユーザー操作、タイマーまたは連動設定により、{terminal_name}を制御中だったため、制御を行いませんでした。\n ※ {control_exec_uer_name}が操作を行いました。"
        else:
            link_terminal_name = hist.get(
                "link_terminal_name", "接点入力" + str(hist.get("link_terminal_no", ""))
            )
            if hist["control_result"] == "success" or hist["control_result"] == "failure":
                msg = f"【画面操作による制御（成功）】\n{terminal_name}の制御信号がデバイスに届き、{link_terminal_name}が{hist.get('link_terminal_state_name')}に変化しました。\n※{control_exec_uer_name}が操作を行いました。"
            elif hist["control_result"] == "timeout_status":
                msg = f"【画面操作による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きましたが、{link_terminal_name}が変化しませんでした。\n※{control_exec_uer_name}が操作を行いました。"
            elif hist["control_result"] == "timeout_response":
                msg = f"【画面操作による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きせんでした。\n※{control_exec_uer_name}が操作を行いました。"
            elif hist["control_result"] == "not_excuted_done":
                msg = f"【画面操作による制御（不実施）】\n他のユーザー操作、タイマーまたは連動設定により、{terminal_name}を制御中だったため、制御を行いませんでした。\n ※ {control_exec_uer_name}が操作を行いました。"

    # タイマー設定による制御
    elif hist["event_type"] == "on_timer_control" or hist["event_type"] == "off_timer_control":
        on_off = "ON" if hist["event_type"] == "on_timer_control" else "OFF"
        terminal_name = hist.get("terminal_name", "接点出力" + str(hist.get("terminal_no", "")))
        link_terminal_name = hist.get(
            "link_terminal_name", "接点入力" + str(hist.get("link_terminal_no", ""))
        )
        if hist["control_result"] == "success" or hist["control_result"] == "failure":
            msg = f"【タイマー設定による制御（成功）】\n制御信号（{terminal_name}）がデバイスに届き、{link_terminal_name}が{hist.get('link_terminal_state_name')}に変化しました。\n※タイマー設定「{on_off}制御　{hist.get('timer_time')}」により制御信号を送信しました。"
        elif hist["control_result"] == "timeout_status":
            msg = f"【タイマー設定による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きましたが、{link_terminal_name}が変化しませんでした。\n※タイマー設定「{on_off}制御　{hist.get('timer_time')}」により制御信号を送信しました。"
        elif hist["control_result"] == "timeout_response":
            msg = f"【タイマー設定による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きませんでした。\n※タイマー設定「{on_off}制御　{hist.get('timer_time')}」により制御信号を送信しました。"
        elif hist["control_result"] == "not_excuted_done":
            msg = f"【タイマー設定による制御（不実施）】\n他のユーザー操作、タイマーまたは連動設定により、{terminal_name}を制御中でした。そのため、制御を行いませんでした。\n※タイマー設定「{on_off}制御　{hist.get('timer_time')}」による制御信号を送信しませんでした。"
        elif (
            hist["control_result"] == "not_excuted_on"
            or hist["control_result"] == "not_excuted_off"
        ):
            msg = f"【タイマー設定による制御（不実施）】\n{link_terminal_name}が既に{hist.get('link_terminal_state_name')}のため、{terminal_name}の制御を行いませんでした。\n※タイマー設定「{on_off}制御　{hist.get('timer_time')}」による制御信号を送信しませんでした。"

    # 連動設定による制御（Ph2）
    elif hist["event_type"] == "linked_control":
        msg = ""

    return msg


def create_response(request_params, hist_list):
    res_hist_list = []
    for hist in hist_list:
        res_hist_list.append(
            {
                "event_datetime": int(hist["event_datetime"] / 1000),
                "recv_datetime": int(hist["recv_datetime"] / 1000)
                if hist.get("recv_datetime")
                else None,
                "device_id": hist["device_id"],
                "device_name": hist["hist_data"].get("device_name"),
                "group_list": hist["hist_data"].get("group_list"),
                "device_imei": hist["hist_data"].get("imei"),
                "event_type": hist["hist_data"].get("event_type"),
                "history_message": create_history_message(hist["hist_data"]),
                "email_notification": "1"
                if hist["hist_data"].get("notification_hist_id")
                else "0",
            }
        )

    return {
        "history": {
            "history_start_datetime": int(request_params["history_start_datetime"]),
            "history_end_datetime": int(request_params["history_end_datetime"]),
            "event_type_list": request_params["event_type_list"],
            "history_list": res_hist_list,
        },
    }


def lambda_handler(event, context):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        try:
            user_info = auth.verify_user(event, user_table)
        except auth.AuthError as e:
            logger.info("ユーザー検証失敗", exc_info=True)
            return {
                "statusCode": e.code,
                "headers": res_headers,
                "body": json.dumps({"message": e.message}, ensure_ascii=False),
            }

        validate_result = validate.validate(
            event,
            user_info,
            account_table,
            user_table,
            contract_table,
            device_relation_table,
        )
        logger.info(validate_result)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        try:
            # 履歴取得
            hist_list = ddb.get_hist_list(hist_list_table, validate_result["request_params"])
            response = create_response(validate_result["request_params"], hist_list)
        except ClientError as e:
            logger.info(e)
            body = {"message": "履歴一覧の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(response, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
