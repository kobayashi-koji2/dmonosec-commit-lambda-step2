import json
import boto3
import ddb
import re
from decimal import Decimal
from aws_lambda_powertools import Logger

# layer
import db
import convert

logger = Logger()


# パラメータチェック
def validate(event, user, device_table, contract_table):
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"message": "ユーザの操作権限がありません。"}
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
    
    # デバイス種別、接点端子数チェック
    for item in device_info:
        logger.info(item["device_type"])
        if (
            item["device_type"] == "PJ1"
            and len(item["device_data"]["config"]["terminal_settings"]["di_list"]) != 1
        ) or (
            item["device_type"] == "PJ2"
            and len(item["device_data"]["config"]["terminal_settings"]["di_list"]) < 1 or len(item["device_data"]["config"]["terminal_settings"]["di_list"]) > 8
        ) or (
            item["device_type"] == "PJ3"
            and len(item["device_data"]["config"]["terminal_settings"]["di_list"]) < 1 or len(item["device_data"]["config"]["terminal_settings"]["di_list"]) > 8
        ) or (
            item["device_type"] == "UnaTag"
        ):
            return {"message": "不正なデバイスIDが指定されています"}
        
    # Bodyパラメータの中身チェック
    for key in body_params["di_event_list"]:
        if key.get("di_no"):
            res_di_no = key["di_no"]
        else:
            return {"message": "パラメータが不正です"}
        
        if key.get("di_state"):
            res_di_state = key["di_state"]
        else:
            return {"message": "パラメータが不正です"}

    if body_params["event_type"] == 0:
        week = body_params["weekday"].split(',')
        for item in week:
            if not "00:00" <= body_params["time"] <= "23:59" or item not in ["","0","1","2","3","4","5","6","7"] or res_di_no not in [1,2,3,4,5,6,7,8] or res_di_state not in [0, 1, 2]:
                    return {"message": "時間、接点入力端子、状態の値が不正です"}
    elif body_params["event_type"] == 1:
        if  not 0 < body_params["elapsed_time"] < 301 or res_di_no not in [1,2,3,4,5,6,7,8] or res_di_state not in [0, 1, 2]:
            return {"message": "継続時間、接点入力端子、状態がの値が不正です"}     
    else:
        return {"message": "イベント種別が不正です"}
        
    # 登録カスタムイベント数チェック（登録の場合、各デバイス10個まで）
    if http_method == "POST":
        for item in device_info:
            if len(item.get("device_data").get("config").get("custom_event_list", [])) >= 10:
                return {"message": "イベントカスタム上限10件に達しています"}
                
    # イベントカスタムID存在チェック 
    if http_method == "PUT":
        custom_event_id_list = list()
        for item in device_info:
            for custom_event_id in item.get("device_data").get("config").get("custom_event_list", []):
                custom_event_id_list.append(custom_event_id["custom_event_id"])
            if body_params["custom_event_id"] not in custom_event_id_list:
                return {"message": "イベントカスタムIDが存在しません"}
            
    return {"custom_info": body_params, "device_id": device_id, "message": ""}


# 操作権限チェック
def operation_auth_check(user):
    user_type = user["user_type"]
    logger.debug(f"権限:{user_type}")
    if user_type == "admin" or user_type == "sub_admin" or user_type == "worker":
        return True
    return False

