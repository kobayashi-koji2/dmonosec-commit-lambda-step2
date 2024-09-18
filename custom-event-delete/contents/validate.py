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
    
    http_method = event.get("httpMethod")
    body_params = json.loads(event.get("body", "{}"))
    pathParam = event.get("pathParameters") or {}
    device_id = pathParam["device_id"]
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
        imei = item["imei"]
        for custom_event_id in item["device_data"]["config"]["custom_event_list"]:
            custom_event_id_list.append(custom_event_id["custom_event_id"])
    if body_params["custom_event_id"] not in custom_event_id_list:
        return {"message": "イベントカスタムIDが存在しません"}
            
    return {"custom_event_id": body_params["custom_event_id"], "device_id": device_id, "imei": imei, "message": ""}

