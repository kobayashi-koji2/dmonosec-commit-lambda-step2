import json
import os
import boto3
import ddb

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

import db
import auth
import ssm
import convert

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user()
def lambda_handler(event, context, user):
    res_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }
    # DynamoDB操作オブジェクト生成
    try:
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        announcement_table = dynamodb.Table(ssm.table_names["ANNOUNCEMENT_TABLE"])
        device_announcement_table = dynamodb.Table(ssm.table_names["DEVICE_ANNOUNCEMENT_TABLE"])
    except KeyError as e:
        body = {"message": e}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }

    try:
        account = db.get_account_info_by_account_id(user.get("account_id"), account_table)
        announcement_flag = ddb.get_announcement_flag(
            announcement_table,
            device_announcement_table,
            user.get("user_data", 0).get("announcement_last_display_datetime", 0),
            user.get("user_type"),
            user.get("contract_id"),
        )

        res_body = {
            "message": "",
            "user_id": user.get("user_id"),
            "email_address": account.get("email_address"),
            "user_type": user.get("user_type"),
            "user_name": account.get("user_data", {}).get("config", {}).get("user_name"),
            "announcement_flag": announcement_flag,
            "display_information": user.get("user_data", {}).get("display_information"),
        }
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
