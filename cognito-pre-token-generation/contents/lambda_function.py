import os
import math

import boto3
from aws_lambda_powertools import Logger

import db
import ssm

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
logger = Logger()


def lambda_handler(event, context):
    try:
        # アカウント情報取得
        account_id = event["userName"]
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        account_info = db.get_account_info_by_account_id(account_id, account_table)
        account = account_info["Item"]

        # パスワード有効期限を計算
        password_update_datetime = account["user_data"]["config"]["password_update_datetime"]
        password_exp = math.ceil(password_update_datetime / 1000) + 90 * 24 * 60 * 60  # 90日

        # クレームにパスワード有効期限を追加
        event["response"]["claimsOverrideDetails"] = {
            "claimsToAddOrOverride": {"password_exp": password_exp}
        }
    except Exception:
        logger.exception(Exception)

    return event
