import json
import os
import boto3
import traceback

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

import auth
import ssm
import db
import convert

import validate

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


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
        except KeyError as e:
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        try:
            login_user = auth.verify_user(event, user_table)
        except auth.AuthError as e:
            logger.info("ユーザー検証失敗", exc_info=True)
            return {
                "statusCode": e.code,
                "headers": res_headers,
                "body": json.dumps({"message": e.message}, ensure_ascii=False),
            }
        # パラメータチェック
        validate_result = validate.validate(event, login_user)
        if validate_result["code"] != "0000":
            return {
                "statusCode": 200,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        user_list = []
        try:
            contract_info = db.get_contract_info(login_user["contract_id"], contract_table)
            for user_id in contract_info.get("contract_data", {}).get("user_list", {}):
                user = db.get_user_info_by_user_id(user_id, user_table)
                account = db.get_account_info_by_account_id(user.get("account_id"), account_table)
                account_config = account.get("user_data", {}).get("config", {})
                user_list.append(
                    {
                        "user_id": user.get("user_id"),
                        "email_address": account.get("email_address"),
                        "user_name": account_config.get("user_name"),
                        "user_type": user.get("user_type"),
                    }
                )
                logger.info(user_list)
        except ClientError as e:
            logger.info(e)
            logger.info(traceback.format_exc())
            body = {"code": "9999", "message": "ユーザ一覧の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        res_body = {"code": "0000", "message": "", "user_list": user_list}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc)
            #'body':res_body
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
