import uuid

from aws_lambda_powertools import Logger

import db
import ddb
import convert
import time
import math
import time
from datetime import datetime, timedelta


logger = Logger()

# カスタムイベント設定登録
def create_custom_event_info(custom_event_info, device_table, device_id,device_state_table):
    device_info = ddb.get_device_info(device_id, device_table)
    device_state = ddb.get_device_state(device_id, device_state_table)
    # カスタムイベントIDの生成
    custom_event_id = str(uuid.uuid4())
    # カスタムイベント登録日時の生成
    custom_event_reg_datetime = math.floor(time.time())
    custom_put_item = dict()
    device_state_put_item = dict()
    # イベントカスタム名チェック
    if custom_event_info["event_type"] == 0:
        if not custom_event_info["custom_event_name"]:
            custom_event_name = "無題の日時カスタムイベント"
            custom_put_item = {
                "custom_event_id": custom_event_id,
                'custom_event_reg_datetime': custom_event_reg_datetime,
                "event_type": custom_event_info["event_type"],
                "custom_event_name": custom_event_name,
                "time": custom_event_info["time"],
                "weekday": custom_event_info["weekday"],
                "di_event_list": custom_event_info["di_event_list"],
            }
        else:
            custom_event_name = custom_event_info["custom_event_name"]
            custom_put_item = {
                "custom_event_id": custom_event_id,
                'custom_event_reg_datetime': custom_event_reg_datetime,
                "event_type": custom_event_info["event_type"],
                "custom_event_name": custom_event_name,
                "time": custom_event_info["time"],
                "weekday": custom_event_info["weekday"],
                "di_event_list": custom_event_info["di_event_list"],
            }
    elif custom_event_info["event_type"] == 1:
        # device_state_put_item = {
        #     "custom_event_id": custom_event_id,
        #     "elapsed_time": custom_event_info["elapsed_time"],
        #     "di_event_list": custom_event_info["di_event_list"],
        # }
        for device_state_custom_event_di in custom_event_info["di_event_list"]:
            di_list = {
                "di_no": device_state_custom_event_di["di_no"],
                "di_state": device_state_custom_event_di["di_state"],
                "event_judge_datetime": 1730782358,
            }
        logger.info(custom_event_id)
        logger.info(custom_event_info["elapsed_time"])
        logger.info(di_list)
        device_state_put_item = {
            "custom_event_id": custom_event_id,
            "elapsed_time": custom_event_info["elapsed_time"],
            "di_event_list": di_list,
        }
        logger.info(device_state_put_item)
        if not custom_event_info["custom_event_name"]:
            custom_event_name = "無題の継続時間カスタムイベント"
            custom_put_item = {
                "custom_event_id": custom_event_id,
                'custom_event_reg_datetime': custom_event_reg_datetime,
                "event_type": custom_event_info["event_type"],
                "custom_event_name": custom_event_name,
                "elapsed_time": custom_event_info["elapsed_time"],
                "di_event_list": custom_event_info["di_event_list"],
            }
        else:
            custom_event_name = custom_event_info["custom_event_name"]
            custom_put_item = {
                "custom_event_id": custom_event_id,
                'custom_event_reg_datetime': custom_event_reg_datetime,
                "event_type": custom_event_info["event_type"],
                "custom_event_name": custom_event_name,
                "elapsed_time": custom_event_info["elapsed_time"],
                "di_event_list": custom_event_info["di_event_list"],
            }
            
    # カスタムイベントリスト追加
    custom_event_list = list()
    for item in device_info:
        imei = item["identification_id"]
        for custom_event in item.get("device_data").get("config").get("custom_event_list", []):
            if custom_event["event_type"] == 0:
                custom_event_item = {
                    "custom_event_id": custom_event["custom_event_id"],
                    'custom_event_reg_datetime': custom_event["custom_event_reg_datetime"],
                    "event_type": custom_event["event_type"],
                    "custom_event_name": custom_event["custom_event_name"],
                    "time": custom_event["time"],
                    "weekday": custom_event["weekday"],
                    "di_event_list": custom_event["di_event_list"],
                }
            if custom_event["event_type"] == 1:
                custom_event_item = {
                    "custom_event_id": custom_event["custom_event_id"],
                    'custom_event_reg_datetime': custom_event["custom_event_reg_datetime"],
                    "event_type": custom_event["event_type"],
                    "custom_event_name": custom_event["custom_event_name"],
                    "elapsed_time": custom_event["elapsed_time"],
                    "di_event_list": custom_event["di_event_list"],
                }
            custom_event_list.append(custom_event_item)
    
    custom_event_list.append(custom_put_item) 
    # custom_event_db_update = update_ddb_custom_event_info(custom_event_list, device_table, device_id, imei)

    # デバイス現状態のカスタムタイマーイベントリスト追加
    device_state_timer_list = list()
    for item in device_state:
        if item.get("custom_timer_event_list",[]) != []:
            for device_state_custom_event in item.get("custom_timer_event_list",[]):
                logger.info("中身あり")
                device_state_custom_event_item = {
                    "custom_event_id": device_state_custom_event["custom_event_id"],
                    "elapsed_time": device_state_custom_event["elapsed_time"],
                    "di_event_list" : device_state_custom_event["di_event_list"],
                }
                device_state_timer_list.append(device_state_custom_event_item)
    logger.info(device_state_timer_list)
    
    device_state_timer_list.append(device_state_put_item) 
    logger.info(device_state_timer_list)
    
    device_state_custom_event_db_update = update_ddb_device_state_info(device_state_timer_list, device_state_table, device_id)
    custom_event_db_update = update_ddb_custom_event_info(custom_event_list, device_table, device_id, imei)
    
    if custom_event_db_update == True:
        if device_state_custom_event_db_update == True:
            res_body = {"message": "データの登録に成功しました。"}
            return True, res_body
        elif device_state_custom_event_db_update == False or not device_state_custom_event_db_update:
            res_body = {"message": "データの登録に失敗しました。"}
            return False, res_body
    else:
        res_body = {"message": "データの登録に失敗しました。"}
        return False, res_body
    
# カスタムイベント設定更新         
def update_custom_event_info(custom_event_info, device_table, device_id,device_state_table):
    device_info = ddb.get_device_info(device_id, device_table)
    device_state = ddb.get_device_state(device_id, device_state_table)
    custom_put_item = dict()
    
    for item in device_info:
        for custom_event in item.get("device_data").get("config").get("custom_event_list", []):
            if custom_event["custom_event_id"] == custom_event_info["custom_event_id"]:
                custom_event_reg_datetime = custom_event["custom_event_reg_datetime"]
            
    if custom_event_info["event_type"] == 0:
        if not custom_event_info["custom_event_name"]:
            custom_event_name = "無題の日時カスタムイベント"
            custom_put_item = {
                "custom_event_id": custom_event_info["custom_event_id"],
                'custom_event_reg_datetime': custom_event_reg_datetime,
                "event_type": custom_event_info["event_type"],
                "custom_event_name": custom_event_name,
                "time": custom_event_info["time"],
                "weekday": custom_event_info["weekday"],
                "di_event_list": custom_event_info["di_event_list"],
            }
        else:
            custom_event_name = custom_event_info["custom_event_name"]
            custom_put_item = {
                "custom_event_id": custom_event_info["custom_event_id"],
                'custom_event_reg_datetime': custom_event_reg_datetime,
                "event_type": custom_event_info["event_type"],
                "custom_event_name": custom_event_name,
                "time": custom_event_info["time"],
                "weekday": custom_event_info["weekday"],
                "di_event_list": custom_event_info["di_event_list"],
            }
    elif custom_event_info["event_type"] == 1:
        for item in device_state:
            for device_state_custom_event in item.get("custom_timer_event_list",[]):
                if device_state_custom_event["custom_event_id"] == custom_event_info["custom_event_id"]:
                    if device_state_custom_event["elapsed_time"]  == custom_event_info["elapsed_time"]:
                        for device_state_custom_event_di in device_state_custom_event["di_event_list"]:
                            di_event_list = {
                                "di_no": device_state_custom_event_di["di_no"],
                                "di_state": device_state_custom_event_di["di_state"],
                                "event_judge_datetime": device_state_custom_event.get("di_event_list").get("event_judge_datetime", ''),
                            }
                        device_state_put_item = {
                            "custom_event_id": custom_event_info["custom_event_id"],
                            "elapsed_time": custom_event_info["elapsed_time"],
                            "di_event_list": di_event_list,
                        }
                    else:
                        sum_event_datetime = datetime.fromtimestamp(int(device_state_custom_event.get("di_event_list").get("event_judge_datetime"))) + timedelta(minutes= custom_event_info["elapsed_time"])
                        event_datetime = sum_event_datetime.timestamp()
                        logger.info(event_datetime)
                        for device_state_custom_event_di in device_state_custom_event.get("di_event_list"):
                            di_event_list = {
                                "di_no": device_state_custom_event_di["di_no"],
                                "di_state": device_state_custom_event_di["di_state"],
                                "event_judge_datetime": device_state_custom_event.get("di_event_list").get("event_judge_datetime", ''),
                                "event_datetime": event_datetime,
                            }
                        device_state_put_item = {
                            "custom_event_id": custom_event_info["custom_event_id"],
                            "elapsed_time": custom_event_info["elapsed_time"],
                            "di_event_list": di_event_list,
                        }
                else:
                    return {"message": "カスタムイベントIDが現状態テーブルに存在しません。"}
        
        if not custom_event_info["custom_event_name"]:
            custom_event_name = "無題の継続時間カスタムイベント"
            custom_put_item = {
                "custom_event_id": custom_event_info["custom_event_id"],
                'custom_event_reg_datetime': custom_event_reg_datetime,
                "event_type": custom_event_info["event_type"],
                "custom_event_name": custom_event_name,
                "elapsed_time": custom_event_info["elapsed_time"],
                "di_event_list": custom_event_info["di_event_list"],
            }
        else:
            custom_event_name = custom_event_info["custom_event_name"]
            custom_put_item = {
                "custom_event_id": custom_event_info["custom_event_id"],
                'custom_event_reg_datetime': custom_event_reg_datetime,
                "event_type": custom_event_info["event_type"],
                "custom_event_name": custom_event_name,
                "elapsed_time": custom_event_info["elapsed_time"],
                "di_event_list": custom_event_info["di_event_list"],
            }
            
    # カスタムイベントリスト更新
    custom_event_list = list()
    for item in device_info:
        imei = item["identification_id"]
        for custom_event in item.get("device_data").get("config").get("custom_event_list", []):
            if custom_event["custom_event_id"] == custom_event_info["custom_event_id"]:
                custom_event = custom_put_item
            custom_event_list.append(custom_event)
    custom_event_db_update = update_ddb_custom_event_info(custom_event_list, device_table, device_id, imei)
            
    # デバイス現状態のカスタムタイマーイベントリスト更新
    device_state_timer_list = list()
    for item in device_state:
        for device_state_custom_event in item.get("custom_timer_event_list",[]):
            if device_state_custom_event["custom_event_id"] == custom_event_info["custom_event_id"]:
                device_state_custom_event = device_state_put_item
            device_state_timer_list.append(device_state_custom_event)
    
    device_state_timer_list.append(device_state_put_item) 
    device_state_custom_event_db_update = update_ddb_device_state_info(device_state_timer_list, device_state_table, device_id)
    
    if custom_event_db_update == True:
        if device_state_custom_event_db_update == True:
            res_body = {"message": "データの登録に成功しました。"}
            return True, res_body
        elif device_state_custom_event_db_update == False or not device_state_custom_event_db_update:
            res_body = {"message": "データの登録に失敗しました。"}
            return False, res_body
    else:
        res_body = {"message": "データの登録に失敗しました。"}
        return False, res_body
    
#　カスタムイベントリスト更新      
def update_ddb_custom_event_info(custom_event_list, device_table, device_id, identification_id):
    put_item_fmt = convert.to_dynamo_format(custom_event_list)
    transact_items = []
    
    custom_event_create = {
        "Update": {
            "TableName": device_table.table_name,
            "Key": {
                "device_id": {"S": device_id},
                "identification_id": {"S": identification_id},
            },
            "UpdateExpression": "set #map_d.#map_c.#map_cel = :s",
            "ExpressionAttributeNames": {
                "#map_d": "device_data",
                "#map_c": "config",
                "#map_cel": "custom_event_list",
            },
            "ExpressionAttributeValues": {
                ":s": put_item_fmt
            },
        }
    }
    transact_items.append(custom_event_create)
        
    logger.debug(f"put_custom_event_info: {transact_items}")
    # 各データを登録・更新
    if not db.execute_transact_write_item(transact_items):
        return False
    else:
        return True

# 現状態テーブルのカスタムタイマーイベントリスト更新
def update_ddb_device_state_info(device_state_timer_list, device_state_table, device_id):
    put_item_fmt = convert.to_dynamo_format(device_state_timer_list)
    transact_items = []
    
    device_state_timer_list_create = {
        "Update": {
            "TableName": device_state_table.table_name,
            "Key": {
                "device_id": {"S": device_id},
            },
            "UpdateExpression": "set #map_c = :s",
            "ExpressionAttributeNames": {
                "#map_c": "custom_timer_event_list",
            },
            "ExpressionAttributeValues": {
                ":s": put_item_fmt
            },
        }
    }
    
    transact_items.append(device_state_timer_list_create)
        
    logger.debug(f"put_custom_event_info: {transact_items}")
    # 各データを登録・更新
    if not db.execute_transact_write_item(transact_items):
        return False
    else:
        return True
    

