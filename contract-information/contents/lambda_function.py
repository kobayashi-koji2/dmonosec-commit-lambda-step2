import json
import os
import traceback

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import convert
import db
import ssm

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
def lambda_handler(event, context, user_info):
    try:
        ##################
        # 0 DynamoDBの操作オブジェクト生成
        ##################
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])

        ##################
        # 2 ユーザー権限確認
        ##################
        if not __operation_auth_check(user_info, ["admin", "sub_admin"]):
            res_body = {"message": "ユーザに操作権限がありません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(
                    res_body, ensure_ascii=False, default=convert.decimal_default_proc
                ),
            }

        ##################
        # 3 契約情報取得
        ##################
        contract_id = user_info["contract_id"]
        contract_info = db.get_contract_info(contract_id, contract_table)
        if not contract_info:
            res_body = {"message": "契約情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(
                    res_body, ensure_ascii=False, default=convert.decimal_default_proc
                ),
            }
        logger.debug(f"contract_info: {contract_info}")

        ##################
        # 4 メッセージ応答
        ##################
        res_body = {
            "message": "",
            "history_storage_period": contract_info["history_storage_period"],
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }


# 操作権限チェック
def __operation_auth_check(user_info, user_type):
    user_id = user_info["user_id"]
    op_user_type = user_info["user_type"]
    logger.debug(f"ユーザID: {user_id}, 権限: {op_user_type}")

    if isinstance(user_type, list):
        result = True if op_user_type in user_type else False
    else:
        result = True if op_user_type == user_type else False

    return result
