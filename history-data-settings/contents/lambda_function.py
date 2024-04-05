import json
import os
import traceback

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import convert
import db
import ssm
import validate

patch_all()
logger = Logger()

# レスポンスヘッダー
res_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}
# AWSリソース定義
dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ["AWS_DEFAULT_REGION"],
    endpoint_url=os.environ.get("endpoint_url"),
)


@auth.verify_login_user()
@validate.validate_request_body
def lambda_handler(event, context, user_info, request_body):
    try:
        # DynamoDB操作オブジェクト生成
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])

        ### 1. 入力情報チェック
        # ユーザー権限確認
        if not validate.operation_auth_check(user_info, ["admin", "sub_admin"]):
            res_body = {"message": "ユーザに操作権限がありません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 2. 履歴保存期間設定更新
        contract_id = user_info["contract_id"]
        contract_info = db.get_contract_info(contract_id, contract_table)
        contract_info["history_storage_period"] = request_body["history_storage_period"]
        put_history_storage_period = [
            {
                "Put": {
                    "TableName": contract_table.table_name,
                    "Item": convert.dict_dynamo_format(contract_info),
                }
            }
        ]
        logger.debug(f"put_history_storage_period: {put_history_storage_period}")
        if not db.execute_transact_write_item(put_history_storage_period):
            res_body = {"message": "DynamoDBへの更新処理に失敗しました。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 3. メッセージ応答
        res_body = {
            "message": "",
            "history_storage_period": request_body["history_storage_period"],
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        logger.error(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
