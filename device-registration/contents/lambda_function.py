import json
import os
import time
import traceback
from datetime import datetime

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import convert
import db
import soracom_api
import ssm
import validate

patch_all()
logger = Logger()

# レスポンスヘッダー
res_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}
# 環境変数
registration_group_id = os.environ["REGISTRATION_GROUP_ID"]
# AWSリソース定義
dynamodb = boto3.resource(
    "dynamodb",
    region_name=os.environ["AWS_DEFAULT_REGION"],
    endpoint_url=os.environ.get("endpoint_url"),
)


@validate.validate_request_body
def lambda_handler(event, context, req_body):
    try:
        ### 0. DynamoDBの操作オブジェクト生成
        operator_table = dynamodb.Table(ssm.table_names["OPERATOR_TABLE"])
        pre_register_table = dynamodb.Table(ssm.table_names["PRE_REGISTER_DEVICE_TABLE"])

        imei = req_body["imei"]
        iccid = req_body["iccid"]

        ### 2. カバレッジ判定
        soracom_id_token = soracom_api.get_soracom_token(operator_table)
        result = soracom_api.get_imsi_info(soracom_id_token, iccid)
        if result.get("message") is not None:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(result, ensure_ascii=False),
            }
        imsi = result["imsi"]
        coverage_url = result["coverage_url"]

        ### 3. IMEIロック
        result = soracom_api.imei_lock(soracom_id_token, imei, iccid, coverage_url)
        if result.get("message") is not None:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(result, ensure_ascii=False),
            }

        result = soracom_api.cancel_lock(soracom_id_token, iccid, coverage_url)
        if result.get("message") is not None:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(result, ensure_ascii=False),
            }

        # ローカル動作確認用
        result = soracom_api.set_group(
            soracom_id_token, iccid, registration_group_id, coverage_url
        )
        if result.get("message") is not None:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(result, ensure_ascii=False),
            }

        ### 4. デバイス登録
        now = datetime.now()
        now_unixtime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)
        put_item = {
            "imei": imei,
            "iccid": iccid,
            "imsi": imsi,
            "device_code": req_body["device_code"],
            "contract_id": req_body["mss_code"],
            "dev_reg_datetime": now_unixtime,
            "contract_state": 0,
            "coverage_url": coverage_url,
        }
        put_item_fmt = convert.dict_dynamo_format(put_item)
        put_pre_register = [
            {
                "Put": {
                    "TableName": pre_register_table.table_name,
                    "Item": put_item_fmt,
                }
            }
        ]
        logger.debug(f"put_pre_register_info: {put_pre_register}")
        if not db.execute_transact_write_item(put_pre_register):
            res_body = {"message": "DynamoDBへの更新処理に失敗しました。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(result, ensure_ascii=False),
            }

        ### 5. メッセージ応答
        res_body = {"message": ""}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
