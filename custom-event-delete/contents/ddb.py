from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

import convert
import db

dynamodb = boto3.resource("dynamodb")
logger = Logger()


def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    )
    return response

def delete_custom_event(device_table, custom_event_id, device_id, imei):
    custom_event_list = list()
    device_info = get_device_info(device_id, device_table).get("Items", {})
    for item in device_info:
        imei = item["imei"]
        for custom_event in item["device_data"]["config"]["custom_event_list"]:
            if custom_event["custom_event_id"] != custom_event_id:
                custom_event_list.append(custom_event)
                
    # カスタムイベント設定削除
    transact_items = []
    custom_event_list_fmt = convert.to_dynamo_format(custom_event_list)
    delete_custom_event_setting = {
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
                ":s": custom_event_list_fmt,
            },
        }
    }
    transact_items.append(delete_custom_event_setting)
    logger.debug(f"delete_custom_event_setting: {transact_items}")
    if not db.execute_transact_write_item(transact_items):
        res_body = {"message": "データの更新に失敗しました。"}
        return False, res_body
    else:
        res_body = {"message": "データの更新に成功しました。"}
        return True, res_body
        

