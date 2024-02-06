import os
import json
import time
import traceback

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
import boto3

# layer
import ssm
import validate
import db
import convert
import auth

patch_all()

logger = Logger()

# 環境変数
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
# 正常レスポンス内容
respons = {
    "headers": {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    },
    "body": "",
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


@auth.verify_login_user(False)
def lambda_handler(event, context, user_info):
    try:
        ### 0. 事前準備
        # DynamoDBの操作オブジェクト生成
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])

        ### 1. 入力情報チェック
        # 入力情報のバリデーションチェック
        val_result = validate.validate(event)
        if val_result.get("message"):
            logger.info("Error in validation check of input information.")
            respons["statusCode"] = 400
            respons["body"] = json.dumps(val_result, ensure_ascii=False)
            return respons
        body = val_result["request_body"]

        ### 2. パスワード変更
        # Cognitoパスワード変更
        response = cognito.change_password(
            PreviousPassword=body["password"],
            ProposedPassword=body["new_password"],
            AccessToken=body["access_token"],
        )

        # パスワード最終更新日時更新
        account_id = user_info["account_id"]
        account_info = db.get_account_info_by_account_id(account_id, account_table)
        if account_info is None:
            res_body = {"message": "アカウント情報が存在しません。"}
            respons["statusCode"] = 404
            respons["body"] = json.dumps(res_body, ensure_ascii=False)
            return respons

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
            respons["statusCode"] = 500
            respons["body"] = json.dumps(res_body, ensure_ascii=False)
            return respons

        ### 3. メッセージ応答
        res_body = {"message": ""}
        respons["statusCode"] = 200
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        respons["statusCode"] = 500
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons
