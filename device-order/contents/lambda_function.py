import os
import json
import traceback
import boto3
from aws_lambda_powertools import Logger

# layer
import ssm
import validate
import db
import convert

logger = Logger()

# 環境変数
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
# 正常レスポンス内容
respons = {
    "statusCode": 200,
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


def lambda_handler(event, context):
    try:
        ### 0. DynamoDBの操作オブジェクト生成
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])

        ### 1. 入力情報チェック
        # 入力情報のバリデーションチェック
        val_result = validate.validate(event, user_table)
        if val_result["code"] != "0000":
            logger.info("Error in validation check of input information.")
            respons["statusCode"] = 500
            respons["body"] = json.dumps(val_result, ensure_ascii=False)
            return respons
        # トークンからユーザー情報取得
        user_info = val_result["user_info"]

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
        res_body = {"code": "0000", "message": "", "device_list": body["device_list"]}
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        respons["statusCode"] = 500
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons
