from aws_lambda_powertools import Logger
import boto3
import db
import convert
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource("dynamodb")
logger = Logger()


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
    