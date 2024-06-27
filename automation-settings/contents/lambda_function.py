import json
import os
import traceback
import uuid
import time
from datetime import datetime

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from botocore.exceptions import ClientError

# layer
import auth
import convert
import db
import ddb
import ssm
import validate

patch_all()
logger = Logger()

# レスポンスヘッダー
res_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}
# AWSリソース定義
dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ["AWS_DEFAULT_REGION"],
    endpoint_url=os.environ.get("endpoint_url"),
)


@auth.verify_login_user()
@validate.validate_parameter
@validate.validate_request_body
def lambda_handler(event, context, user_info, trigger_device_id, request_body):
    try:
        # DynamoDB操作オブジェクト生成
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        automation_table = dynamodb.Table(ssm.table_names["AUTOMATION_TABLE"])

        control_device_id = request_body["control_device_id"]

        ### 1. 入力情報チェック
        # ユーザー権限確認
        if not validate.operation_auth_check(user_info, "referrer", False):
            res_body = {"message": "ユーザに操作権限がありません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 2. デバイス種別チェック(共通)
        trigger_device_info = db.get_device_info_other_than_unavailable(
            trigger_device_id, device_table
        )
        control_device_info = db.get_device_info_other_than_unavailable(
            control_device_id, device_table
        )
        if not trigger_device_info or not control_device_info:
            res_body = {"message": "デバイス情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.debug(f"trigger_device_info: {trigger_device_info}")
        logger.debug(f"control_device_info: {control_device_info}")

        if request_body.get("trigger_event_type") in ["di_change_state", "di_unhealthy"]:
            if (
                trigger_device_info["device_type"] == "PJ1"
                and request_body.get("trigger_terminal_no") not in [1]
            ) or (
                trigger_device_info["device_type"] == "PJ2"
                and request_body.get("trigger_terminal_no") not in [1, 2]
            ):
                res_body = {"message": "トリガー端子番号が不正です。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }

        if control_device_info["device_type"] != "PJ2":
            res_body = {"message": "デバイス種別が想定と一致しません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        if request_body.get("control_do_no") not in [1, 2]:
            res_body = {"message": "コントロール接点出力端子が不正です。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 3. デバイス操作権限チェック(共通)
        contract_info = db.get_contract_info(user_info["contract_id"], contract_table)
        logger.debug(f"contract_info: {contract_info}")
        device_id_list = contract_info.get("contract_data", {}).get("device_list", [])
        if trigger_device_id not in device_id_list or control_device_id not in device_id_list:
            res_body = {"message": "デバイス操作権限がありません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 4. デバイス操作権限チェック(ユーザ権限が作業者の場合)
        if user_info["user_type"] == "worker":
            user_devices = db.get_user_relation_device_id_list(
                user_info["user_id"], device_relation_table
            )
            if trigger_device_id not in user_devices or control_device_id not in user_devices:
                res_body = {"message": "デバイス操作権限がありません。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
            logger.debug(f"device_relation_info: {user_devices}")

        ### 5. 重複チェック
        automation_info = ddb.get_automation_info_device(trigger_device_id, automation_table)
        for item in automation_info:
            if item["automation_id"] != request_body.get("automation_id") and (
                item.get("trigger_event_type") == request_body["trigger_event_type"]
                and item.get("control_device_id") == control_device_id
                and item.get("control_do_no") == request_body.get("control_do_no")
                and item.get("control_di_state") == request_body.get("control_di_state")
                and (
                    (
                        item["trigger_event_type"] == "di_change_state"
                        and item.get("trigger_terminal_no")
                        == request_body.get("trigger_terminal_no")
                        and item.get("trigger_event_detail_state")
                        == request_body.get("trigger_event_detail_state")
                    )
                    or (
                        item["trigger_event_type"] == "di_unhealthy"
                        and item.get("trigger_terminal_no")
                        == request_body.get("trigger_terminal_no")
                        and item.get("trigger_event_detail_flag")
                        == request_body.get("trigger_event_detail_flag")
                    )
                    or (
                        item["trigger_event_type"] not in ["di_change_state", "di_unhealthy"]
                        and item.get("trigger_event_detail_flag")
                        == request_body.get("trigger_event_detail_flag")
                    )
                )
            ):
                res_body = {"message": "同じ設定が重複しています。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }

        ### 6. 連動制御設定
        # 連動制御設定新規登録
        if event["httpMethod"] == "POST":
            flag, result = create_automation_setting(
                trigger_device_id, request_body, automation_table
            )
            if not flag:
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(result, ensure_ascii=False),
                }
        # 連動制御設定更新
        elif event["httpMethod"] == "PUT":
            flag, result = update_automation_setting(
                trigger_device_id, request_body, automation_table
            )
            if not flag:
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(result, ensure_ascii=False),
                }

        ### 7. 連動制御設定情報取得
        automation_info = ddb.get_automation_info_device(trigger_device_id, automation_table)
        if user_info["user_type"] == "worker":
            # ユーザに紐づくデバイスID取得
            automation_info = [
                automation
                for automation in automation_info
                if automation["control_device_id"] in set(user_devices)
            ]

        ### 8. メッセージ応答
        automation_list = list()
        for item in automation_info:
            do_automation_item = {
                "automation_id": item["automation_id"],
                "automation_reg_datetime": item.get("automation_reg_datetime", 0),
                "automation_name": item["automation_name"],
                "control_device_id": item["control_device_id"],
                "trigger_event_type": item["trigger_event_type"],
                "trigger_terminal_no": item.get("trigger_terminal_no"),
                "trigger_event_detail_state": item.get("trigger_event_detail_state"),
                "trigger_event_detail_flag": item.get("trigger_event_detail_flag"),
                "control_do_no": item["control_do_no"],
                "control_di_state": item["control_di_state"],
            }
            if item["trigger_event_type"] == "di_change_state":
                do_automation_item["trigger_event_detail_state"] = item[
                    "trigger_event_detail_state"
                ]
            else:
                do_automation_item["trigger_event_detail_flag"] = item["trigger_event_detail_flag"]
            automation_list.append(do_automation_item)

        res_body = {"message": "", "automation_list": automation_list}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        logger.error(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }


def create_automation_setting(trigger_device_id, request_body, automation_table):

    # 連動制御設定数の確認（上限100）
    automation_info = ddb.get_automation_info_device(trigger_device_id, automation_table)
    if len(automation_info) >= 100:
        res_body = {"message": "設定可能なオートメーション設定の上限を超えています。"}
        return False, res_body

    # 連動制御設定の追加
    event_type = request_body["trigger_event_type"]
    now = datetime.now()
    now_unixtime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)
    put_item = {
        "automation_id": str(uuid.uuid4()),
        "automation_reg_datetime": now_unixtime,
        "automation_name": request_body["automation_name"],
        "trigger_device_id": trigger_device_id,
        "trigger_event_type": request_body["trigger_event_type"],
        "trigger_terminal_no": request_body.get("trigger_terminal_no", 0),
        "control_device_id": request_body["control_device_id"],
        "control_do_no": request_body["control_do_no"],
        "control_di_state": request_body["control_di_state"],
    }
    if event_type == "di_change_state":
        put_item["trigger_event_detail_state"] = request_body["trigger_event_detail_state"]
    else:
        put_item["trigger_event_detail_flag"] = request_body["trigger_event_detail_flag"]

    put_item_fmt = convert.dict_dynamo_format(put_item)
    put_automation = [
        {
            "Put": {
                "TableName": automation_table.table_name,
                "Item": put_item_fmt,
            }
        }
    ]
    logger.debug(f"put_automation_info: {put_automation}")
    # 各データを登録・更新・削除
    if not db.execute_transact_write_item(put_automation):
        res_body = {"message": "DynamoDBへの更新処理に失敗しました。"}
        return False, res_body

    return True, request_body


def update_automation_setting(trigger_device_id, request_body, automation_table):

    # 連動制御設定の更新
    update_expression = "SET #an = :an, #tdi = :tdi, #ttn = :ttn, #ctd = :ctd, #cdo = :cdo, #cds = :cds, #teds = :teds, #tedf = :tedf"
    expression_attribute_names = {
        "#an": "automation_name",
        "#tdi": "trigger_device_id",
        "#ttn": "trigger_terminal_no",
        "#ctd": "control_device_id",
        "#cdo": "control_do_no",
        "#cds": "control_di_state",
        "#teds": "trigger_event_detail_state",
        "#tedf": "trigger_event_detail_flag",
    }
    expression_attribute_values = {
        ":an": request_body["automation_name"],
        ":tdi": trigger_device_id,
        ":ttn": request_body.get("trigger_terminal_no", 0),
        ":ctd": request_body["control_device_id"],
        ":cdo": request_body["control_do_no"],
        ":cds": request_body["control_di_state"],
        ":teds": request_body.get("trigger_event_detail_state"),
        ":tedf": request_body.get("trigger_event_detail_flag"),
    }
    expression_attribute_values_fmt = convert.dict_dynamo_format(expression_attribute_values)
    update_automation = [
        {
            "Update": {
                "TableName": automation_table.table_name,
                "Key": {"automation_id": {"S": request_body["automation_id"]}},
                "UpdateExpression": update_expression,
                "ExpressionAttributeNames": expression_attribute_names,
                "ExpressionAttributeValues": expression_attribute_values_fmt,
            }
        }
    ]
    logger.debug(f"update_automation_info: {update_automation}")
    # 各データを登録・更新・削除
    if not db.execute_transact_write_item(update_automation):
        res_body = {"message": "DynamoDBへの更新処理に失敗しました。"}
        return False, res_body

    return True, request_body
