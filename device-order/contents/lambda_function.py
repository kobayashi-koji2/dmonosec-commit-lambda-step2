import os
import json
import traceback
import boto3
from aws_lambda_powertools import Logger

# layer
import auth
import ssm
import validate
import db
import convert

logger = Logger()

# 環境変数
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]

# レスポンスヘッダー
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


@auth.verify_login_user
def lambda_handler(event, context, user_info):
    try:
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

        ### 2. デバイス表示順序更新
        # ユーザー情報取得
        # 1月まではいったん、ログインするユーザーIDとモノセコムユーザーIDは同じ認識で直接ユーザー管理より参照する形で実装
        # バリデーションチェックの処理の中でモノセコムユーザー管理より参照しているのでその値を使用

        # デバイス表示順序更新
        body = val_result["req_body"]
        af_device_list = body["device_list"]
        af_user_data = user_info["user_data"]
        af_user_data["config"]["device_order"] = af_device_list
        af_user_data = convert.to_dynamo_format(af_user_data)
        transact_items = [
            {
                "Update": {
                    "TableName": ssm.table_names["USER_TABLE"],
                    "Key": {"user_id": {"S": user_info["user_id"]}},
                    "UpdateExpression": "set #s = :s",
                    "ExpressionAttributeNames": {"#s": "user_data"},
                    "ExpressionAttributeValues": {":s": af_user_data},
                }
            }
        ]
        logger.info(transact_items)
        result = db.execute_transact_write_item(transact_items)

        ### 3. メッセージ応答
        res_body = {"message": "", "device_list": body["device_list"]}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
    except Exception as e:
        logger.info(e)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
