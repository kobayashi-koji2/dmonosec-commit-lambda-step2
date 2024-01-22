import json
import os

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

import auth
import db
import ddb
import ssm
import validate

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
cognito = boto3.client("cognito-idp")


@auth.verify_login_user
@validate.validate_parameter
def lambda_handler(event, context, user, body):
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

    try:
        account = db.get_account_info(user["user_id"], account_table)
        if account is None:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "アカウント情報が存在しません。"}, ensure_ascii=False),
            }

        try:
            cognito.verify_user_attribute(
                AccessToken=body["access_token"], AttributeName="email", Code=body["auth_code"]
            )
        except ClientError:
            logger.error("ユーザー属性(email)の検証に失敗", exc_info=True)
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "ユーザー属性(email)の検証に失敗しました。"}, ensure_ascii=False),
            }

        ddb.update_account_email(account["account_id"], body["new_email"], account_table)

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
