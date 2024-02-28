import json
import os

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

import auth
import convert
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
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # 画面表示情報更新
        try:
            updated_display_information = ddb.update_user_display_information(
                user_id=user["user_id"],
                display_information=body["display_information"],
                user_table=user_table
            )
        except ClientError as e:
            logger.info(f"画面表示情報更新エラー e={e}")
            res_body = {"message": "画面表示情報の更新に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        res_body = {
            "message": "",
            "display_information": updated_display_information
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
