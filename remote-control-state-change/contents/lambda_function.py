import json
import os
import time
import traceback

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
import boto3

import auth
import ddb
import db
import ssm


patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

# レスポンスヘッダー
response_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}


@auth.verify_login_user()
def lambda_handler(event, context, user_info):
    try:
        # DynamoDB操作オブジェクト生成
        try:
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            remote_control_table = dynamodb.Table(ssm.table_names["REMOTE_CONTROL_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": response_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        logger.info(user_info)

        # 権限が参照者の場合はエラー
        if user_info["user_type"] == "referrer":
            return {
                "statusCode": 400,
                "headers": response_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        # 通信制御情報取得
        user_id = user_info["user_id"]
        device_req_no = event["pathParameters"]["device_req_no"]
        logger.info(f"device_req_no: {device_req_no}")
        remote_control = db.get_remote_control(device_req_no, remote_control_table)

        logger.info(f"remote_control: {remote_control}")

        if remote_control is None:
            return {
                "statusCode": 404,
                "headers": response_headers,
                "body": json.dumps({"message": "端末要求が存在しません。"}, ensure_ascii=False),
            }

        device_id = remote_control["device_id"]

        # デバイス操作権限チェック（共通）
        contract = db.get_contract_info(user_info["contract_id"], contract_table)
        logger.info(f"contract: {contract}")
        device_id_list_by_contract = contract["contract_data"]["device_list"]

        if device_id not in device_id_list_by_contract:
            return {
                "statusCode": 400,
                "headers": response_headers,
                "body": json.dumps(
                    {"message": "端末の操作権限がありません。"}, ensure_ascii=False
                ),
            }

        # デバイス操作権限チェック（管理者, 副管理者でない場合）
        if user_info["user_type"] not in ["admin", "sub_admin"]:
            allowed_device_id_list = db.get_user_relation_device_id_list(
                user_id, device_relation_table
            )
            logger.info(f"allowed_device_id_list: {allowed_device_id_list}")

            if device_id not in allowed_device_id_list:
                return {
                    "statusCode": 400,
                    "headers": response_headers,
                    "body": json.dumps(
                        {"message": "端末の操作権限がありません。"},
                        ensure_ascii=False,
                    ),
                }

        # 状態変化通知確認
        recv_datetime = remote_control["recv_datetime"]
        limit_datetime = recv_datetime + 20000  # 20秒

        logger.info(int(time.time() * 1000))
        while int(time.time() * 1000) <= limit_datetime:
            remote_control = db.get_remote_control(device_req_no, remote_control_table)
            if remote_control.get("link_di_result") is not None:
                control_result = "0" if remote_control["link_di_result"] == "0" else "1"
                return {
                    "statusCode": 200,
                    "headers": response_headers,
                    "body": json.dumps(
                        {
                            "message": "",
                            "device_req_no": device_req_no,
                            "control_result": control_result,
                        },
                        ensure_ascii=False,
                    ),
                }
            time.sleep(1)

        return {
            "statusCode": 200,
            "headers": response_headers,
            "body": json.dumps(
                {
                    "message": "",
                    "device_req_no": device_req_no,
                    "control_result": "1",
                },
                ensure_ascii=False,
            ),
        }

    except Exception as e:
        logger.info(e)
        return {
            "statusCode": 500,
            "headers": response_headers,
            "body": json.dumps(
                {"message": "予期しないエラーが発生しました。"}, ensure_ascii=False
            ),
        }
