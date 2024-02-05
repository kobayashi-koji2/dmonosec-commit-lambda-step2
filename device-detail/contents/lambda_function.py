import json
import boto3
import validate
import generate_detail
import ddb
import os
import re
from botocore.exceptions import ClientError
from decimal import Decimal
import traceback
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import db
import ssm
import convert
import auth

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


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
                "user_table": dynamodb.Table(ssm.table_names["USER_TABLE"]),
                "device_table": dynamodb.Table(ssm.table_names["DEVICE_TABLE"]),
                "group_table": dynamodb.Table(ssm.table_names["GROUP_TABLE"]),
                "device_state_table": dynamodb.Table(ssm.table_names["STATE_TABLE"]),
                "account_table": dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"]),
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

        ##################
        # 1 入力情報チェック
        ##################
        validate_result = validate.validate(event, user_info, tables)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }
        device_id = validate_result["device_id"]
        logger.info(f"デバイスID:{device_id}")

        ##################
        # 4 デバイス情報取得
        ##################
        try:
            # 4.1 デバイス設定取得
            device_info = ddb.get_device_info(device_id, tables["device_table"]).get("Items", {})
            if len(device_info) == 0:
                res_body = {"message": "デバイス情報が存在しません。"}
                return {
                    "statusCode": 404,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
            elif len(device_info) >= 2:
                res_body = {
                    "message": "デバイスIDに「契約状態:初期受信待ち」「契約状態:使用可能」の機器が複数紐づいています",
                }
                return {
                    "statusCode": 500,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
            # 4.2 デバイス現状態取得
            device_state = db.get_device_state(device_id, tables["device_state_table"])
            # 4.3 グループ情報取得
            group_id_list = db.get_device_relation_group_id_list(
                device_id, tables["device_relation_table"]
            )
            group_info_list = []
            for group_id in group_id_list:
                group_info = db.get_group_info(group_id, tables["group_table"])
                if group_info:
                    group_info_list.append(group_info)
            # 4.4 デバイス詳細情報生成
            res_body = num_to_str(
                generate_detail.get_device_detail(device_info[0], device_state, group_info_list)
            )
        except ClientError as e:
            logger.info(e)
            body = {"message": "デバイス詳細の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        logger.info(f"レスポンスボディ:{res_body}")
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }


def num_to_str(obj):
    if isinstance(obj, dict):
        return {key: num_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [num_to_str(item) for item in obj]
    elif isinstance(obj, (int, float, Decimal)):
        return str(obj)
    else:
        return obj
