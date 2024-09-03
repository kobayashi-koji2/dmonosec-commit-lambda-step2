import uuid

from aws_lambda_powertools import Logger

import db
import ddb
import convert


logger = Logger()

# カスタムイベント設定登録
def create_custom_event_info(custom_event_info, device_table, device_id):
    device_info = ddb.get_device_info(device_id, device_table).get("Items", {})
    # カスタムイベントIDの生成
    custom_event_id = str(uuid.uuid4())
# イベントカスタム名チェック
    if custom_event_info["custom_event_name"]:
        custom_event_name = custom_event_info["custom_event_name"]
    elif not custom_event_info["custom_event_name"] and custom_event_info["event_type"] == 0:
        custom_event_name = "無題の日時カスタムイベント"
    elif  not custom_event_info["custom_event_name"] and custom_event_info["event_type"] == 1:
        custom_event_name = "無題の継続時間カスタムイベント"
    put_item = {
        "custom_event_id": custom_event_id,
        'custom_event_reg_datetime': custom_event_info["custom_event_reg_datetime"],
        "event_type": custom_event_info["event_type"],
        "custom_event_name": custom_event_name,
        "time": custom_event_info["time"],
        "weekday": custom_event_info["weekday"],
        "elapsed_time": custom_event_info["elapsed_time"],
        "di_event_list": custom_event_info["di_event_list"],
    }
    custom_event_list = list()
    for item in device_info:
        imei = item["imei"]
        for custom_event in item["device_data"]["config"]["custom_event_list"]:
            custom_event_item = {
                "custom_event_id": custom_event["custom_event_id"],
                "custom_event_reg_datetime": custom_event["custom_event_reg_datetime"],
                "event_type": custom_event["event_type"],
                "custom_event_name": custom_event["custom_event_name"],
                "time": custom_event["time"],
                "weekday": custom_event["weekday"],
                "elapsed_time": custom_event["elapsed_time"],
                "di_event_list": custom_event["di_event_list"],
            }
            logger.info(custom_event_item)
            custom_event_list.append(custom_event_item)
            
    custom_event_list.append(put_item) 
    
    db_update = update_ddb_custom_event_info(custom_event_list, device_table, device_id, imei)
    
    if db_update == True:
        res_body = {"message": "データの登録に成功しました。"}
        return True, res_body
    else:
        res_body = {"message": "データの登録に失敗しました。"}
        return False, res_body
    
# カスタムイベント設定更新         
def update_custom_event_info(custom_event_info, device_table, device_id):
    device_info = ddb.get_device_info(device_id, device_table).get("Items", {})
    if custom_event_info["custom_event_name"]:
        custom_event_name = custom_event_info["custom_event_name"]
    elif not custom_event_info["custom_event_name"] and custom_event_info["event_type"] == 0:
        custom_event_name = "無題の日時カスタムイベント"
    elif  not custom_event_info["custom_event_name"] and custom_event_info["event_type"] == 1:
        custom_event_name = "無題の継続時間カスタムイベント"
    put_item = {
        "custom_event_id": custom_event_info["custom_event_id"],
        'custom_event_reg_datetime': custom_event_info["custom_event_reg_datetime"],
        "event_type": custom_event_info["event_type"],
        "custom_event_name": custom_event_name,
        "time": custom_event_info["time"],
        "weekday": custom_event_info["weekday"],
        "elapsed_time": custom_event_info["elapsed_time"],
        "di_event_list": custom_event_info["di_event_list"],
    }
    custom_event_list = list()
    for item in device_info:
        imei = item["imei"]
        for custom_event in item["device_data"]["config"]["custom_event_list"]:
            if custom_event["custom_event_id"] == custom_event_info["custom_event_id"]:
                custom_event = put_item
            custom_event_list.append(custom_event)    
            
    db_update = update_ddb_custom_event_info(custom_event_list, device_table, device_id, imei)
    
    if db_update == True:
        res_body = {"message": "データの更新に成功しました。"}
        return True, res_body
    else:
        res_body = {"message": "データの更新に失敗しました。"}
        return False, res_body
            
def update_ddb_custom_event_info(custom_event_list, device_table, device_id, imei):
    put_item_fmt = convert.to_dynamo_format(custom_event_list)
    logger.info(put_item_fmt)
    transact_items = []
    
    custom_event_create = {
        "Update": {
            "TableName": device_table.table_name,
            "Key": {
                "device_id": {"S": device_id},
                "imei": {"S": imei},
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
    

