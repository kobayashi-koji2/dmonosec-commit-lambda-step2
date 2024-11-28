import json
import os
import secrets
import string
from datetime import datetime
from dateutil import relativedelta

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

import auth
import db
import mail
import ssm
import validate
import ddb

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

AWS_REGION = os.environ.get("AWS_REGION")
COGNITO_USER_POOL_ID = os.environ["COGNITO_USER_POOL_ID"]
ENDPOINT_URL = os.environ.get("endpoint_url")
TEMPORARY_PASSWORD_PERIOD_DAYS = int(os.environ["TEMPORARY_PASSWORD_PERIOD_DAYS"])


def get_random_password_string(length):
    pass_chars = string.ascii_letters + string.digits + string.punctuation
    while True:
        password = "".join(secrets.choice(pass_chars) for x in range(length))
        upper = sum(c.isupper() for c in password)
        lower = sum(c.islower() for c in password)
        digit = sum(c.isdigit() for c in password)
        punctuation = sum(c in string.punctuation for c in password)
        if upper >= 1 and lower >= 1 and digit >= 1 and punctuation >= 1:
            break
    return password


@auth.verify_login_user()
@validate.validate_parameter
def lambda_handler(event, context, login_user, user_id):
    res_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }
    logger.info({"login_user": login_user})
    logger.info({"user_id": user_id})

    # DynamoDB操作オブジェクト生成
    try:
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
    except KeyError as e:
        body = {"message": e}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }

    try:
        # 権限チェック
        if login_user["user_type"] not in ["admin", "sub_admin"]:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        contract = db.get_contract_info(login_user["contract_id"], contract_table)
        logger.info({"contract": contract})

        if user_id not in contract.get("contract_data", {}).get("user_list", []):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(
                    {"message": "削除されたユーザーが選択されました。\n画面の更新を行います。\n\nエラーコード：006-0106"}, ensure_ascii=False
                ),
            }

        # 通知対象ユーザーの存在チェック
        user = db.get_user_info_by_user_id(user_id, user_table)
        account = db.get_account_info_by_account_id(user["account_id"], account_table)
        logger.info({"user": user})
        if not account:
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps({"message": "削除されたユーザーが選択されました。\n画面の更新を行います。\n\nエラーコード：006-0106"}, ensure_ascii=False),
            }

        # ユーザーの認証状態をチェック
        account_config = account.get("user_data", {}).get("config", {})
        if account_config.get("auth_status") != "unauthenticated":
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(
                    {"message": "ユーザーは未認証状態ではありません。"}, ensure_ascii=False
                ),
            }

        # 初期パスワード更新・通知
        try:
            client = boto3.client(
                "cognito-idp",
                region_name=AWS_REGION,
                endpoint_url=ENDPOINT_URL,
            )
            client.admin_create_user(
                UserPoolId=COGNITO_USER_POOL_ID,
                Username=account["email_address"],
                MessageAction="RESEND",
                DesiredDeliveryMediums=["EMAIL"],
                TemporaryPassword=get_random_password_string(8),
            )
        except ClientError:
            logger.error("初期パスワード通知エラー", exc_info=True)
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(
                    {"message": "初期パスワード通知に失敗しました。"}, ensure_ascii=False
                ),
            }

        # 初期パスワード有効期限を更新
        auth_period = int(
            (
                datetime.now() + relativedelta.relativedelta(days=TEMPORARY_PASSWORD_PERIOD_DAYS)
            ).timestamp()
            * 1000
        )
        try:
            ddb.update_account_auth_period(account["account_id"], auth_period, account_table)
        except ClientError as e:
            logger.info(f"初期パスワード有効期限更新エラー e={e}")
            res_body = {"message": "初期パスワード有効期限の更新に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps({"message": ""}, ensure_ascii=False),
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
