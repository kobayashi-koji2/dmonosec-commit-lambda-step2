import json
import boto3
import ddb
import generate_detail
import re
from decimal import Decimal
from aws_lambda_powertools import Logger

# layer
import db

logger = Logger()


# パラメータチェック
def validate(event, user_info, tables):
    headers = event.get("headers", {})
    pathParam = event.get("pathParameters", {})
    if not headers or not pathParam:
        return {"message": "パラメータが不正です。"}
    if "Authorization" not in headers or "device_id" not in pathParam:
        return {"message": "パラメータが不正です。"}
    device_id = pathParam["device_id"]
    contract_id = user_info["contract_id"]  # フェーズ2以降削除
    contract_info = db.get_contract_info(contract_id, tables["contract_table"])
    if not contract_info:
        return {"message": "アカウント情報が存在しません。"}

    ##################
    # 2 デバイス操作権限チェック(共通)
    # 3 デバイス操作権限チェック(ユーザ権限が作業者、参照者の場合)
    ##################
    operation_auth = operation_auth_check(user_info, contract_info, device_id, tables)
    if not operation_auth:
        return {"message": "不正なデバイスIDが指定されています"}

    return {"user_info": user_info, "device_id": device_id}


# 操作権限チェック
def operation_auth_check(user_info, contract_info, device_id, tables):
    user_type, user_id = user_info["user_type"], user_info["user_id"]
    # 2.1 デバイスID一覧取得
    accunt_devices = contract_info["contract_data"]["device_list"]
    # 2.2 デバイス操作権限チェック
    if device_id not in accunt_devices:
        return False

    if user_type == "worker" or user_type == "referrer":
        # 3.1 ユーザに紐づくデバイスID取得
        device_id_list = db.get_user_relation_device_id_list(
            user_id, tables["device_relation_table"]
        )
        if device_id not in set(device_id_list):
            return False
    return True
