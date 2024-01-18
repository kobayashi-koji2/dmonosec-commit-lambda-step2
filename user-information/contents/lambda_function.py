import json
import os
import boto3

from aws_lambda_powertools import Logger

import db
import auth
import ssm
import convert

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user
def lambda_handler(event, context, user):
    res_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }
    # DynamoDB操作オブジェクト生成
    try:
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
    except KeyError as e:
        body = {"message": e}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }

    try:
        account = db.get_account_info_by_account_id(user.get("account_id"), account_table)

        res_body = {
            "message": "",
            "user_id": user.get("user_id"),
            "email_address": account.get("email_address"),
            "user_type": user.get("user_type"),
            "last_list_page": user.get("user_data", {}).get("config", {}).get("last_page"),
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
