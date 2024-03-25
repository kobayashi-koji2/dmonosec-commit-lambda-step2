import json
import boto3
import validate
import os
import ddb
from botocore.exceptions import ClientError
import traceback
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import convert
import ssm

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]


@auth.verify_login_user()
def lambda_handler(event, context, user_info):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            tables = {
                "device_table": dynamodb.Table(ssm.table_names["DEVICE_TABLE"]),
                "group_table": dynamodb.Table(ssm.table_names["GROUP_TABLE"]),
                "device_state_table": dynamodb.Table(ssm.table_names["STATE_TABLE"]),
                "contract_table": dynamodb.Table(ssm.table_names["CONTRACT_TABLE"]),
                "device_relation_table": dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"]),
            }
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェックおよびimei取得
        validate_result = validate.validate(event, user_info, tables)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }
        # タイマー設定削除
        body = validate_result["body"]
        device_id = validate_result["device_id"]
        imei = validate_result["imei"]
        convert_param = convert.float_to_decimal(body)
        logger.info(f"デバイスID:{device_id}")
        logger.info(f"IMEI:{imei}")

        try:
            ddb.delete_timer_settings(device_id, imei, convert_param, tables["device_table"])
        except ClientError as e:
            logger.info(f"タイマー設定の削除エラー e={e}")
            res_body = {"message": "タイマー設定の削除に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(
                    res_body, ensure_ascii=False, default=convert.decimal_default_proc
                ),
            }
        res_body = {
            "message": "",
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
