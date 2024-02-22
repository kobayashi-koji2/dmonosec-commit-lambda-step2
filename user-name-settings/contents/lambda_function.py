import json
import os

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

import auth
import ddb
import ssm
import validate

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user()
@validate.validate_parameter
def lambda_handler(event, context, user, body):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        logger.info({"user": user})

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

        # ユーザー名更新
        try:
            updated_user_name = ddb.update_account_user_name(
                account_id=user["account_id"],
                user_name=body["user_name"],
                account_table=account_table
            )
        except ClientError as e:
            logger.info(f"ユーザー名更新エラー e={e}")
            res_body = {"message": "ユーザー名の更新に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        res_body = {
            "message": "",
            "user_name": updated_user_name
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
