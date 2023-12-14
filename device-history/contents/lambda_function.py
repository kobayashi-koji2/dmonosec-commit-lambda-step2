import json
import os
import logging
import traceback

import boto3
from botocore.exceptions import ClientError

# layer
import convert
import ssm

import ddb
import validate

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

parameter = None
logger = logging.getLogger()


def create_history_message(hist):
    msg = ""
    # 接点入力変化
    if hist["event_type"] == "di_change":
        terminal_name = hist.get(
            "terminal_name", "接点入力" + str(hist.get("terminal_no", ""))
        )
        msg = f"【接点入力変化】\n{terminal_name}が{hist['terminal_state_name']}に変化しました。"

    # 接点出力変化
    elif hist["event_type"] == "do_change":
        terminal_name = hist.get(
            "terminal_name", "接点出力" + str(hist.get("terminal_no", ""))
        )
        msg = f"【接点出力変化】\n{terminal_name}が{hist['terminal_state_name']}に変化しました。"

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
            msg = "【FW異常（発生）】\nFW更新異常が発生しました。"
        elif hist["occurrence_flag"] == 0:
            msg = "【FW異常（復旧）】\nFW更新異常が復旧しました。"

    # 電源ON
    elif hist["event_type"] == "power_on":
        msg = "【電源ON】\nデバイスの電源がONになりました。"

    # デバイスヘルシー未受信（Ph2？）
    elif hist["event_type"] == "device_unhealthy":
        msg = ""

    # 接点入力未変化検出（Ph2）
    elif hist["event_type"] == "di_unhealthy":
        msg = ""

    # 画面操作による制御
    elif hist["event_type"] == "manual_control":
        # TODO 要求仕様更新を待ってから対応
        msg = ""

    # タイマー設定による制御
    elif (
        hist["event_type"] == "on_timer_control"
        or hist["event_type"] == "off_timer_control"
    ):
        # TODO 要求仕様更新を待ってから対応
        msg = ""

    # 連動設定による制御（Ph2）
    elif hist["event_type"] == "linked_control":
        msg = ""

    return msg


def create_response(request_params, hist_list):
    res_hist_list = []
    for hist in hist_list:
        res_hist_list.append(
            {
                "event_datetime": hist["event_datetime"],
                "recv_datetime": hist["recv_datetime"],
                "device_id": hist["device_id"],
                "device_name": hist["hist_data"].get("device_name"),
                "device_imei": hist["hist_data"].get("imei"),
                "event_type": hist["hist_data"].get("event_type"),
                "history_message": create_history_message(hist["hist_data"]),
                "email_notification": "1"
                if hist["hist_data"].get("notification_hist_id")
                else "0",
            }
        )

    return {
        "code": "0000",
        "history": {
            "history_start_datetime": request_params["history_start_datetime"],
            "history_end_datetime": request_params["history_end_datetime"],
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
        # コールドスタートの場合パラメータストアから値を取得してグローバル変数にキャッシュ
        global parameter
        if not parameter:
            print("try ssm get parameter")
            response = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(response)
            print("tried ssm get parameter")
        else:
            print("passed ssm get parameter")
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(parameter["USER_TABLE"])
            account_table = dynamodb.Table(parameter.get("ACCOUNT_TABLE"))
            contract_table = dynamodb.Table(parameter.get("CONTRACT_TABLE"))
            hist_list_table_table = dynamodb.Table(parameter.get("HIST_LIST_TABLE"))
        except KeyError as e:
            parameter = None
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        validate_result = validate.validate(
            event, account_table, user_table, contract_table
        )
        print(validate_result)
        if validate_result["code"] != "0000":
            return {
                "statusCode": 200,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        try:
            # 履歴取得
            hist_list = ddb.get_hist_list(
                hist_list_table_table, validate_result["request_params"]
            )
            response = create_response(validate_result["request_params"], hist_list)
        except ClientError as e:
            print(e)
            print(traceback.format_exc())
            body = {"code": "9999", "message": "履歴一覧の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(
                response, ensure_ascii=False, default=convert.decimal_default_proc
            ),
        }
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
