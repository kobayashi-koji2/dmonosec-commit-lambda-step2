import json
import os
from datetime import datetime, timezone
import traceback
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

import auth
import convert
import ssm
import ddb
import validate

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


def create_history_message(hist):
    msg = ""
    # 接点入力変化
    if hist["event_type"] == "di_change":
        terminal_name = hist.get("terminal_name", "接点入力" + str(hist.get("terminal_no", "")))
        msg = f"【接点入力変化】\n{terminal_name}が{hist["terminal_state_name"]}に変化しました。"

    # 接点入力未変化検出（Ph2）
    elif hist["event_type"] == "di_unhealthy":
        terminal_name = hist.get("terminal_name", "接点出力" + str(hist.get("terminal_no", "")))
        if hist["occurrence_flag"] == 1:
            if hist["di_healthy_type"] == "hour":
                healthy_period = str(hist["di_healthy_period"]) + "時間"
            elif hist["di_healthy_type"] == "day":
                healthy_period = str(hist["di_healthy_period"]) + "日"
            msg = f"【接点未入力変化検出（発生）】\n設定した期間（{healthy_period}）、{terminal_name}の変化信号を受信しませんでした。"
        elif hist["occurrence_flag"] == 0:
            msg = f"【接点未入力変化検出（復旧）】\n{terminal_name}の変化信号を受信しました。"

    # 接点出力変化
    elif hist["event_type"] == "do_change":
        terminal_name = hist.get("terminal_name", "接点出力" + str(hist.get("terminal_no", "")))
        msg = f"【接点出力変化】\n{terminal_name}が{hist["terminal_state_name"]}に変化しました。"

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

    # 機器異常（Ph2）
    elif hist["event_type"] == "device_unhealthy":
        if hist["occurrence_flag"] == 1:
            healthy_period = str(hist["device_healthy_period"]) + "日"
            msg = f"【信号未受信異常（発生）】\n設定した期間（{healthy_period}）、デバイスから信号を受信しませんでした。"
        elif hist["occurrence_flag"] == 0:
            msg = "【信号未受信異常（復旧）】\nデバイスから信号を受信しました。"

    # FW更新異常
    elif hist["event_type"] == "fw_update_abnormality":
        if hist["occurrence_flag"] == 1:
            msg = "【FW更新異常（発生）】\nFW更新異常が発生しました。"
        elif hist["occurrence_flag"] == 0:
            msg = "【FW更新異常（復旧）】\nFW更新異常が復旧しました。"

    # 電源ON
    elif hist["event_type"] == "power_on":
        msg = "【電源ON】\nデバイスの電源がONになりました。"

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
            elif hist["control_result"] == "timeout_status":
                msg = f"【画面操作による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きましたが、{link_terminal_name}が変化しませんでした。\n※{control_exec_uer_name}が操作を行いました。"
            elif hist["control_result"] == "not_excuted_done":
                msg = f"【画面操作による制御（不実施）】\n他のユーザー操作、タイマーまたは連動設定により、{terminal_name}を制御中だったため、制御を行いませんでした。\n ※{control_exec_uer_name}が操作を行いました。"
        else:
            link_terminal_name = hist.get(
                "link_terminal_name", "接点入力" + str(hist.get("link_terminal_no", ""))
            )
            if hist["control_result"] == "success" or hist["control_result"] == "failure":
                msg = f"【画面操作による制御（成功）】\n制御信号（{terminal_name}）がデバイスに届き、{link_terminal_name}が{hist.get("link_terminal_state_name")}に変化しました。\n※{control_exec_uer_name}が操作を行いました。"
            elif hist["control_result"] == "timeout_status":
                msg = f"【画面操作による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きましたが、{link_terminal_name}が変化しませんでした。\n※{control_exec_uer_name}が操作を行いました。"
            elif hist["control_result"] == "timeout_response":
                msg = f"【画面操作による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きせんでした。\n※{control_exec_uer_name}が操作を行いました。"
            elif hist["control_result"] == "not_excuted_done":
                msg = f"【画面操作による制御（不実施）】\n他のユーザー操作、タイマーまたは連動設定により、{terminal_name}を制御中だったため、制御を行いませんでした。\n ※{control_exec_uer_name}が操作を行いました。"

    # タイマー設定による制御
    elif hist["event_type"] in ["timer_control", "on_timer_control", "off_timer_control"]:
        on_off = ""
        if hist["event_type"] == "on_timer_control":
            on_off = "ON制御　"
        elif hist["event_type"] == "off_timer_control":
            on_off = "OFF制御　"
        terminal_name = hist.get("terminal_name", "接点出力" + str(hist.get("terminal_no", "")))
        if not hist.get("link_terminal_no"):
            if hist["control_result"] == "success" or hist["control_result"] == "failure":
                msg = f"【タイマー設定による制御（成功）】\n制御信号（{terminal_name}）がデバイスに届きました。\n※タイマー設定「{on_off}{hist.get("timer_time")}」により制御信号を送信しました。"
            elif hist["control_result"] == "timeout_response":
                msg = f"【タイマー設定による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きませんでした。\n※タイマー設定「{on_off}{hist.get("timer_time")}」により制御信号を送信しました。"
            elif hist["control_result"] == "not_excuted_done":
                msg = f"【タイマー設定による制御（不実施）】\n他のユーザー操作、タイマーまたは連動設定により、{terminal_name}を制御中でした。そのため、制御を行いませんでした。\n※タイマー設定「{on_off}{hist.get("timer_time")}」による制御信号を送信しませんでした。"
        else:
            link_terminal_name = hist.get(
                "link_terminal_name", "接点入力" + str(hist.get("link_terminal_no", ""))
            )
            if hist["control_result"] == "success" or hist["control_result"] == "failure":
                msg = f"【タイマー設定による制御（成功）】\n制御信号（{terminal_name}）がデバイスに届き、{link_terminal_name}が{hist.get("link_terminal_state_name")}に変化しました。\n※タイマー設定「{on_off}{hist.get("timer_time")}」により制御信号を送信しました。"
            elif hist["control_result"] == "timeout_response":
                msg = f"【タイマー設定による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きませんでした。\n※タイマー設定「{on_off}{hist.get("timer_time")}」により制御信号を送信しました。"
            elif hist["control_result"] == "timeout_status":
                msg = f"【タイマー設定による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きましたが、{link_terminal_name}が変化しませんでした。\n※タイマー設定「{on_off}{hist.get("timer_time")}」により制御信号を送信しました。"
            elif hist["control_result"] == "not_excuted_done":
                msg = f"【タイマー設定による制御（不実施）】\n他のユーザー操作、タイマーまたは連動設定により、{terminal_name}を制御中でした。そのため、制御を行いませんでした。\n※タイマー設定「{on_off}{hist.get("timer_time")}」による制御信号を送信しませんでした。"
            elif (
                hist["control_result"] == "not_excuted_on"
                or hist["control_result"] == "not_excuted_off"
            ):
                msg = f"【タイマー設定による制御（不実施）】\n{link_terminal_name}が既に{hist.get("link_terminal_state_name")}のため、{terminal_name}の制御を行いませんでした。\n※タイマー設定「{on_off}{hist.get("timer_time")}」による制御信号を送信しませんでした。"

    # 連動設定による制御（Ph2）
    elif hist["event_type"] in ["automation_control", "on_automation_control", "off_automation_control"]:
        terminal_name = hist.get("terminal_name", "接点出力" + str(hist.get("terminal_no", "")))
        device_name = hist.get("automation_trigger_device_name", hist["automation_trigger_imei"])
        event_type_label, event_detail_label = automation_setting(hist)
        if not hist.get("link_terminal_no"):
            if hist["control_result"] in ["success", "falure"]:
                msg = f"【連動設定による制御（成功）】\n制御信号（{terminal_name}）がデバイスに届きました。\n※連動設定「{device_name}、{event_type_label}、{event_detail_label}」により制御信号を送信しました。"
            elif hist["control_result"] == "timeout_response":
                msg = f"【連動設定による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きませんでした。\n※連動設定「{device_name}、{event_type_label}、{event_detail_label}」により制御信号を送信しました。"
            elif hist["control_result"] == "not_excuted_done":
                msg = f"【連動設定による制御（不実施）】\n他のユーザー操作、タイマーまたは連動設定により、{terminal_name}を制御中でした。\nそのため、制御を行いませんでした。\n※連動設定「{device_name}、{event_type_label}、{event_detail_label}」により制御信号を送信しませんでした。"
        else:
            link_terminal_name = hist["link_terminal_name"]
            link_terminal_state_name = hist["link_terminal_state_name"]
            if hist["control_result"] in ["success", "falure"]:
                msg = f"【連動設定による制御（成功）】\n制御信号（{terminal_name}）がデバイスに届き、{link_terminal_name}が{link_terminal_state_name}に変化しました。\n※連動設定「{device_name}、{event_type_label}、{event_detail_label}」により制御信号を送信しました。"
            elif hist["control_result"] == "timeout_status":
                msg = f"【連動設定による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きましたが、{link_terminal_name}が変化しませんでした。\n※連動設定「{device_name}、{event_type_label}、{event_detail_label}」により制御信号を送信しました。"
            elif hist["control_result"] == "timeout_response":
                msg = f"【連動設定による制御（失敗）】\n制御信号（{terminal_name}）がデバイスに届きませんでした。\n※連動設定「{device_name}、{event_type_label}、{event_detail_label}」により制御信号を送信しました。"
            elif hist["control_result"] == "not_excuted_done":
                msg = f"【連動設定による制御（不実施）】\n他のユーザー操作、タイマーまたは連動設定により、{terminal_name}を制御中でした。\nそのため、制御を行いませんでした。\n※連動設定「{device_name}、{event_type_label}、{event_detail_label}」により制御信号を送信しませんでした。"
            elif hist["control_result"] == "not_excuted":
                msg = f"【連動設定による制御（不実施）】\n{link_terminal_name}が既に{link_terminal_state_name}のため、{terminal_name}の制御を行いませんでした。\n※連動設定「{device_name}、{event_type_label}、{event_detail_label}」により制御信号を送信しませんでした。"

    return msg


def create_response(request_params, hist_list):
    res_hist_list = []
    device_last_hist_id_pair = {}
    for hist in hist_list:
        group_list = hist["hist_data"].get("group_list", [])
        if group_list:
            group_list = sorted(group_list, key=lambda x:x['group_name'])
        res_hist_list.append(
            {
                "event_datetime": int(hist["event_datetime"] / 1000),
                "recv_datetime": (
                    int(hist["recv_datetime"] / 1000) if hist.get("recv_datetime") else None
                ),
                "device_id": hist["device_id"],
                "device_name": hist["hist_data"].get("device_name"),
                "group_list": group_list,
                "device_imei": hist["hist_data"].get("imei"),
                "event_type": hist["hist_data"].get("event_type"),
                "history_message": create_history_message(hist["hist_data"]),
                "email_notification": (
                    "1" if hist["hist_data"].get("notification_hist_id") else "0"
                ),
            }
        )
        device_last_hist_id_pair[hist["device_id"]] = hist["hist_id"]

    device_list = [{"device_id": device_id, "last_hist_id": last_hist_id} for device_id, last_hist_id in device_last_hist_id_pair.items()]
    logger.debug(device_list)

    return {
        "history": {
            "history_start_datetime": int(request_params["history_start_datetime"]),
            "history_end_datetime": int(request_params["history_end_datetime"]),
            "event_type_list": request_params["event_type_list"],
            "history_list": res_hist_list,
            "device_list": device_list,
        },
    }


def automation_setting(hist):
    event_type = hist["automation_trigger_event_type"]
    if event_type == "di_change_state":
        event_type_label = (
            "接点入力" + str(hist.get("automation_trigger_terminal_no", "")) + "(接点状態)"
        )
    elif event_type == "di_unhealthy":
        event_type_label = (
            "接点入力" + str(hist.get("automation_trigger_terminal_no", "")) + "(変化検出状態)"
        )
    elif event_type == "device_unhealthy":
        event_type_label = "デバイスヘルシー未受信"
    elif event_type == "battery_near":
        event_type_label = "バッテリーニアエンド"
    elif event_type == "device_abnormality":
        event_type_label = "機器異常"
    elif event_type == "parameter_abnormality":
        event_type_label = "パラメータ異常"
    elif event_type == "fw_update_abnormality":
        event_type_label = "FW更新異常"
    elif event_type == "power_on":
        event_type_label = "電源ON"

    if event_type == "di_change_state":
        if hist["automation_trigger_event_detail_state"] == 1:
            event_detail_label = "オープン"
        else:
            event_detail_label = "クローズ"
    elif event_type == "di_unhealthy":
        if hist["automation_trigger_event_detail_flag"] == 0:
            event_detail_label = "接点入力検出復旧"
        else:
            event_detail_label = "接点入力未変化検出"
    else:
        if hist["automation_trigger_event_detail_flag"] == 0:
            event_detail_label = "正常"
        else:
            event_detail_label = "異常"

    return event_type_label, event_detail_label


@auth.verify_login_user()
def lambda_handler(event, context, user_info):
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
        logger.error(traceback.format_exc())
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
