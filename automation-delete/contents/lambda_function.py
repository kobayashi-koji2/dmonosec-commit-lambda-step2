import json
import os
import traceback
import uuid

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import convert
import db
import ssm
import validate

import ddb

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

        ### 1. 入力情報チェック
        # ユーザー権限確認
        if not validate.operation_auth_check(user_info, "referrer", False):
            res_body = {"message": "閲覧ユーザーは操作権限がありません\n\nエラーコード：003-0706"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 2. 連動制御設定取得
        automation_info = ddb.get_automation_info(request_body["automation_id"], automation_table)
        if not automation_info:
            res_body = {"message": "削除されたオートメーションが選択されました。\n画面の更新を行います。\n\nエラーコード：003-0701"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 3. デバイス種別チェック(共通)
        device_info = db.get_device_info_other_than_unavailable(
            automation_info.get("control_device_id"), device_table
        )
        if not device_info:
            res_body = {"message": "デバイス情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.debug(f"device_info: {device_info}")
        if device_info["device_type"] != "PJ2":
            res_body = {"message": "デバイス種別が想定と一致しません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 4. デバイス操作権限チェック(共通)
        contract_info = db.get_contract_info(user_info["contract_id"], contract_table)
        logger.debug(f"contract_info: {contract_info}")
        device_id_list = contract_info.get("contract_data", {}).get("device_list", [])
        if (
            trigger_device_id not in device_id_list
            or automation_info.get("control_device_id") not in device_id_list
        ):
            res_body = {"message": "デバイス操作権限がありません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 5. デバイス操作権限チェック(ユーザ権限が作業者の場合)
        if user_info["user_type"] == "worker":
            user_devices = db.get_user_relation_device_id_list(
                user_info["user_id"], device_relation_table
            )
            if (
                trigger_device_id not in user_devices
                or automation_info.get("control_device_id") not in user_devices
            ):
                res_body = {"message": "デバイス操作権限がありません。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
            logger.debug(f"device_relation_info: {user_devices}")

        ### 6. 連動制御設定削除
        delete_automation_setting = [
            {
                "Delete": {
                    "TableName": automation_table.table_name,
                    "Key": {
                        "automation_id": {"S": request_body["automation_id"]},
                    },
                    "ConditionExpression": "#cdi = :cdi",
                    "ExpressionAttributeNames": {"#cdi": "trigger_device_id"},
                    "ExpressionAttributeValues": {":cdi": {"S": trigger_device_id}},
                }
            }
        ]
        logger.debug(f"delete_automation_setting: {delete_automation_setting}")
        if not db.execute_transact_write_item(delete_automation_setting):
            res_body = {"message": "DynamoDBへの更新処理に失敗しました。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 7. メッセージ応答
        res_body = {"message": ""}
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
