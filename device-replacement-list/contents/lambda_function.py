import json
import os
import traceback

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import db
import ssm
import validate
import ddb

patch_all()

logger = Logger()

# 環境変数
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]

# レスポンスヘッダー
res_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}

# AWSリソース定義
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_DEFAULT_REGION,
    endpoint_url=os.environ.get("endpoint_url"),
)


@auth.verify_login_user()
@validate.validate_parameter
def lambda_handler(event, context, user_info, identification_id):
    try:
        pre_register_table = dynamodb.Table(ssm.table_names["PRE_REGISTER_DEVICE_TABLE"])
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])

        ### 1. 入力情報チェック
        # ユーザー権限確認
        operation_auth = operation_auth_check(user_info)
        if not operation_auth:
            res_body = {"message": "ユーザに操作権限がありません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        pre_device_info = ddb.get_pre_reg_device_info_by_imei(identification_id, pre_register_table)
        device_type = ""
        if pre_device_info.get("device_code") == "MS-C0100":
            device_type = "PJ1"
        elif pre_device_info.get("device_code") == "MS-C0110":
            device_type = "PJ2"
        elif pre_device_info.get("device_code") == "MS-C0130":
            device_type = "UnaTag"

        ### 2. 保守交換対象デバイス一覧取得
        # デバイス一覧取得
        contract_info = db.get_contract_info(user_info["contract_id"], contract_table)
        if not contract_info:
            res_body = {"message": "契約情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        device_id_list = contract_info["contract_data"]["device_list"]

        # デバイス情報取得
        device_list = list()
        for device_id in device_id_list:
            device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
            if device_info is None:
                continue

            if device_info.get("device_type") != device_type:
                continue

            # 保守交換対象デバイス一覧生成
            if device_type == "UnaTag":
                result = {
                    "device_id": device_info["device_id"],
                    "device_name": device_info["device_data"]["config"]["device_name"],
                    "sigfox_id": device_info["sigfox_id"],
                }
            else:
                result = {
                    "device_id": device_info["device_id"],
                    "device_name": device_info["device_data"]["config"]["device_name"],
                    "device_imei": device_info["imei"],
                }
            device_list.append(result)

        ### 3. メッセージ応答
        res_body = {"message": "", "device_list": device_list}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }


# 操作権限チェック
def operation_auth_check(user_info):
    user_type = user_info["user_type"]
    logger.debug(f"ユーザー権限: {user_type}")
    if user_type == "admin" or user_type == "sub_admin":
        return True
    return False
