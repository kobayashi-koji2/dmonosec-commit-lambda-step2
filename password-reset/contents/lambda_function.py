import os
import json
import time
import traceback

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
import boto3
import botocore

# layer
import ssm
import validate
import db
import convert

patch_all()

logger = Logger()

# 環境変数
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
COGNITO_USER_POOL_CLIENT_ID = os.environ["COGNITO_USER_POOL_CLIENT_ID"]
# 正常レスポンスヘッダー内容
res_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}
# AWSリソース定義
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_DEFAULT_REGION,
    endpoint_url=os.environ.get("endpoint_url"),
)
cognito = boto3.client(
    "cognito-idp",
    region_name=AWS_DEFAULT_REGION,
    endpoint_url=os.environ.get("endpoint_url"),
)


def lambda_handler(event, context):
    try:
        ### 0. 事前準備
        # DynamoDBの操作オブジェクト生成
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])

        ### 1. 入力情報チェック
        # 入力情報のバリデーションチェック
        val_result = validate.validate(event)
        if val_result.get("message"):
            logger.info("Error in validation check of input information.")
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(val_result, ensure_ascii=False),
            }
        body = val_result["request_body"]

        ### 2. Cognitoパスワード再設定
        try:
            cognito.confirm_forgot_password(
                ClientId=COGNITO_USER_POOL_CLIENT_ID,
                Username=body["email_address"],
                ConfirmationCode=body["auth_code"],
                Password=body["new_password"],
            )
        except cognito.exceptions.CodeMismatchException:
            res_body = {"message": "認証コードが違います。\n認証コードをご確認のうえ、もう一度入力してください。\n\nエラーコード：002-0501"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        except cognito.exceptions.ExpiredCodeException:
            res_body = {
                "message": "認証コードの有効期限が切れています。\nパスワード再設定メールの送信をもう一度行ってください。\n\nエラーコード：002-0503"
            }
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 3. パスワード最終更新日時更新
        account_info = db.get_account_info_by_email_address(body["email_address"], account_table)
        if account_info is None:
            res_body = {"message": "アカウント情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        af_user_data = account_info["user_data"]
        af_user_data["config"]["password_update_datetime"] = int(time.time() * 1000)
        af_user_data = convert.to_dynamo_format(af_user_data)

        transact_items = [
            {
                "Update": {
                    "TableName": ssm.table_names["ACCOUNT_TABLE"],
                    "Key": {"account_id": {"S": account_info["account_id"]}},
                    "UpdateExpression": "set #s = :s",
                    "ExpressionAttributeNames": {"#s": "user_data"},
                    "ExpressionAttributeValues": {":s": af_user_data},
                }
            }
        ]
        logger.info(f"transact_items: {transact_items}")
        result = db.execute_transact_write_item(transact_items)
        if not result:
            res_body = {"message": "アカウント情報への書き込みに失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 4. メッセージ応答
        res_body = {"message": ""}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
