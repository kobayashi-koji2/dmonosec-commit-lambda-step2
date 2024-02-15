import json
import os
import time
import traceback
import uuid

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import convert
import db
import ddb
import ssm
import validate

patch_all()

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


@auth.verify_login_user()
@validate.validate_request_body
def lambda_handler(event, context, user_info, request_body):
    try:
        ### 0. DynamoDBの操作オブジェクト生成
        pre_register_table = dynamodb.Table(ssm.table_names["PRE_REGISTER_DEVICE_TABLE"])
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])

        ### 1. 入力情報チェック
        # ユーザー権限確認
        operation_auth = __operation_auth_check(user_info)
        if not operation_auth:
            res_body = {"message": "ユーザに操作権限がありません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        device_id = request_body["device_id"]
        af_device_imei = request_body["device_imei"]

        transact_items = list()
        ### 2. デバイス情報更新
        pre_device_info = ddb.get_pre_reg_device_info_by_imei(af_device_imei, pre_register_table)
        if not pre_device_info:
            res_body = {"message": "登録前デバイス情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.debug(f"pre_device_info: {pre_device_info}")

        device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
        if device_info is None:
            res_body = {"message": "保守交換対象のデバイス情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.debug(f"device_info: {device_info}")

        # 新デバイス情報登録
        put_item = {
            "device_id": device_id,
            "imei": af_device_imei,
            "contract_state": 0,  # 「0:初期受信待ち」で値は固定
            "device_type": device_info["device_type"],
            "device_data": {
                "param": {
                    "iccid": pre_device_info["iccid"],
                    "device_code": pre_device_info["device_code"],
                    "contract_id": pre_device_info["contract_id"],
                    "dev_reg_datetime": int(pre_device_info["dev_reg_datetime"]),
                    "coverage_url": pre_device_info["coverage_url"],
                    "use_type": 1,  # 「1:保守交換」で値は固定
                    "dev_user_reg_datetime": int(time.time() * 1000),
                    "service": "monosc",
                },
                "config": device_info["device_data"][
                    "config"
                ],  # デバイスの「設定情報」は元々の値を引継ぐ
            },
        }
        put_item_fmt = convert.dict_dynamo_format(put_item)
        put_device = {
            "Put": {
                "TableName": ssm.table_names["DEVICE_TABLE"],
                "Item": put_item_fmt,
            }
        }
        transact_items.append(put_device)
        logger.debug(f"put_device_info: {put_device}")

        # 旧デバイス情報更新（契約状態を「2:利用不可」へ更新）
        bf_device_imei = device_info["imei"]
        update_contract = {
            "Update": {
                "TableName": ssm.table_names["DEVICE_TABLE"],
                "Key": {"device_id": {"S": device_id}, "imei": {"S": bf_device_imei}},
                "UpdateExpression": "SET #name1 = :value1",
                "ExpressionAttributeNames": {"#name1": "contract_state"},
                "ExpressionAttributeValues": {":value1": {"N": "2"}},
            }
        }
        transact_items.append(update_contract)
        logger.debug(f"update_bf_device_info: {update_contract}")

        ### 3. デバイス関連情報更新
        # IMEI情報更新
        put_item = {
            "imei": af_device_imei,
            "contract_id": pre_device_info["contract_id"],
            "device_id": device_id,
        }
        put_item_fmt = convert.dict_dynamo_format(put_item)
        put_imei = {
            "Put": {
                "TableName": ssm.table_names["IMEI_TABLE"],
                "Item": put_item_fmt,
            }
        }
        transact_items.append(put_imei)
        logger.debug(f"put_imei: {put_imei}")

        # ICCID情報更新
        put_item = {
            "iccid": pre_device_info["iccid"],
            "contract_id": pre_device_info["contract_id"],
            "device_id": device_id,
        }
        put_item_fmt = convert.dict_dynamo_format(put_item)
        put_iccid = {
            "Put": {
                "TableName": ssm.table_names["ICCID_TABLE"],
                "Item": put_item_fmt,
            }
        }
        transact_items.append(put_iccid)
        logger.debug(f"put_iccid: {put_iccid}")

        ### 4. 登録前デバイス情報削除
        delete_pre_register = {
            "Delete": {
                "TableName": ssm.table_names["PRE_REGISTER_DEVICE_TABLE"],
                "Key": {"imei": {"S": af_device_imei}},
            }
        }
        transact_items.append(delete_pre_register)
        logger.debug(f"delete_pre_register: {delete_pre_register}")

        # 各データを登録・更新・削除
        if not db.execute_transact_write_item(transact_items):
            res_body = {"message": "DynamoDBへの更新処理に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
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


# 操作権限チェック
def __operation_auth_check(user_info):
    user_type = user_info["user_type"]
    logger.debug(f"ユーザー権限: {user_type}")
    if user_type == "admin" or user_type == "sub_admin":
        return True
    return False
