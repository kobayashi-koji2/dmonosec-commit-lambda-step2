import os
import math

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

import db
import ssm

patch_all()

COGNITO_USER_POOL_ID = os.environ["COGNITO_USER_POOL_ID"]

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
logger = Logger()


def lambda_handler(event, context):
    try:
        # アカウント情報取得
        auth_id = event["request"].get("userAttributes").get("custom:auth_id", "")
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        account = db.get_account_info(auth_id, account_table)

        # cognitoユーザー情報取得
        cognito_user = get_cognito_user(auth_id)

        # パスワード有効期限を計算
        password_update_datetime = account["user_data"]["config"]["password_update_datetime"]
        password_exp = math.ceil(password_update_datetime / 1000) + 90 * 24 * 60 * 60  # 90日

        # クレームにパスワード有効期限を追加
        event["response"]["claimsOverrideDetails"] = {
            "claimsToAddOrOverride": {"password_exp": password_exp},
            "mfa_flag": account["user_data"]["config"].get("mfa_flag", 0),
            "cognito_mfa_flag": 1 if cognito_user["UserMFASettingList"] else 0,
        }
    except Exception:
        logger.exception(Exception)

    return event


def get_cognito_user(auth_id):
    client = boto3.client(
        "cognito-idp",
        region_name=os.environ.get("AWS_REGION"),
        endpoint_url=os.environ.get("endpoint_url"),
    )

    response = client.admin_get_user(UserPoolId=COGNITO_USER_POOL_ID, Username=auth_id)
    return response
