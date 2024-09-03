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
    device_info = ddb.get_device_info(device_id, device_table).get("Items", {})
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
    # Bodyパラメータの中身チェック
    custom_event_id_list = []
    for key in body_params["di_event_list"]:
        res_di_no = key["di_no"]
        res_di_state = key["di_state"]
    if body_params["event_type"] == 0:
        if not body_params["time"] or not body_params["weekday"] or not res_di_no or not res_di_state:
            return {"message": "時間、曜日、接点入力端子、状態が指定されていません"}
    elif body_params["event_type"] == 1:
        if not body_params["elapsed_time"] or not key["di_no"] or not res_di_no or not res_di_state:
            return {"message": "継続時間、接点入力端子、状態が指定されていません"}
    else:
        return {"message": "イベントタイプが不正です"}
        
    # 登録カスタムイベント数チェック（登録の場合、各デバイス10個まで）
    if http_method == "POST":
        for item in device_info:
            if len(item["device_data"]["config"]["custom_event_list"]) >= 10:
                return {"message": "イベントカスタム上限10件に達しています"}
            
    # イベントカスタムID存在チェック 
    if http_method == "PUT":
        custom_event_id_list = list()
        for item in device_info:
            for custom_event_id in item["device_data"]["config"]["custom_event_list"]:
                custom_event_id_list.append(custom_event_id["custom_event_id"])
        if body_params["custom_event_id"] not in custom_event_id_list:
            return {"message": "イベントカスタムIDが存在しません"}
            
    return {"custom_info": body_params, "device_id": device_id, "message": ""}

