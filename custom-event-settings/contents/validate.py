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
    logger.info(body_params)
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
    for key in body_params["di_event_list"]:
        res_di_no = key["di_no"]
        res_di_state = key["di_state"]
    if not body_params["custom_event_name"] or not body_params["event_type"]:
        return {"message": "カスタムイベント名、イベントタイプは必須です。（TBD）"}
    elif body_params["event_type"] == 0:
        logger.info("日時指定の場合")
        if not body_params["time"] or not body_params["weekday"] or not res_di_no or not res_di_state:
            return {"message": "日時指定の場合、時間、曜日、DIイベントリストは必須です。（TBD）"}
    elif body_params["event_type"] == 1:
        logger.info("継続時間指定の場合")
        if not body_params["elapsed_time"] or not key["di_no"] or not res_di_no or not res_di_state:
            return {"message": "継続時間指定の場合、継続時間、DIイベントリストは必須です。（TBD）"}
        
    # 登録カスタムイベント数チェック（登録の場合、各デバイス10個まで）
    if http_method == "POST":
        logger.info("デバイス登録数上限チェック")
        for item in device_info:
            if len(item["device_data"]["config"]["custom_event_list"]) >= 10:
                return {"message": "該当デバイスのカスタムイベント数が登録数の上限に達しています（TBD）"}
            
    return {"custom_info": body_params, "device_id": device_id, "message": ""}

