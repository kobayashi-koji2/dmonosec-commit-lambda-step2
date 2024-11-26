import json
import boto3
import ddb
import re
from decimal import Decimal
from aws_lambda_powertools import Logger

# layer
import db

logger = Logger()


# パラメータチェック
def validate(event, user, device_table, contract_table):
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"message": "閲覧ユーザーは操作権限がありません。\n\nエラーコード：003-0806"}
    http_method = event.get("httpMethod")
    body_params = json.loads(event.get("body", "{}"))
    pathParam = event.get("pathParameters") or {}
    device_id = pathParam["device_id"]
    if not device_id:
        return {"message": "リクエストパラメータが不正です。"}
    device_info = ddb.get_device_info(device_id, device_table)
    # パラメータの中身チェック
    if not http_method or not device_id:
        return {"message": "パラメータが不正です。"}
    
    # 契約IDがあるかチェック
    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"message": "アカウント情報が存在しません。"}
    
    # デバイスIDの権限チェック
    if device_id not in contract["contract_data"]["device_list"]:
        return {"message": "不正なデバイスIDが指定されています。"}
        
    # イベントカスタムID存在チェック
    custom_event_id_list = list()
    for item in device_info:
        identification_id = item["identification_id"]
        for custom_event_id in item["device_data"]["config"]["custom_event_list"]:
            custom_event_id_list.append(custom_event_id["custom_event_id"])
    if body_params["custom_event_id"] not in custom_event_id_list:
        return {"message": "削除されたカスタムイベントが選択されました。\n画面の更新を行います。\n\nエラーコード：003-0801"}
            
    return {"custom_event_id": body_params["custom_event_id"], "device_id": device_id, "identification_id": identification_id, "message": ""}

# 操作権限チェック
def operation_auth_check(user):
    user_type = user["user_type"]
    logger.debug(f"権限:{user_type}")
    if user_type == "admin" or user_type == "sub_admin" or user_type == "worker":
        return True
    return False

