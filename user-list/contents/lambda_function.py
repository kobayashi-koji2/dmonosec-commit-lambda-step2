import json
import os
import time

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from botocore.exceptions import ClientError

import auth
import ssm
import db
import convert

import validate

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


@auth.verify_login_user()
def lambda_handler(event, context, login_user):
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
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(event, login_user)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        user_list = []
        try:
            contract_info = db.get_contract_info(login_user["contract_id"], contract_table)
            for user_id in contract_info.get("contract_data", {}).get("user_list", {}):
                user = db.get_user_info_by_user_id(user_id, user_table)
                if user.get("user_type") == "admin":
                    continue

                account = db.get_account_info_by_account_id(user.get("account_id"), account_table)
                account_config = account.get("user_data", {}).get("config", {})

                auth_status = account_config.get("auth_status")
                if auth_status == "unauthenticated":
                    if account_config.get("auth_period", 0) / 1000 < int(time.time()):
                        auth_status = "expired"

                user_list.append(
                    {
                        "user_id": user.get("user_id"),
                        "email_address": account.get("email_address"),
                        "user_name": account_config.get("user_name"),
                        "user_type": user.get("user_type"),
                        "auth_status": auth_status,
                    }
                )
                logger.info(user_list)
        except ClientError as e:
            logger.info(e)
            body = {"message": "ユーザ一覧の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        res_body = {"message": "", "user_list": user_list}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
