from decimal import Decimal
import db

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr


logger = Logger()
dynamodb = boto3.resource("dynamodb")


# デバイス情報取得(契約状態:使用不可以外)
def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    ).get("Items", {})
    return db.add_imei_in_device_info_list(response)


# デバイス設定更新
def update_device_settings(device_id, imei, device_settings, table):
    map_attribute_name = "device_data"
    sub_attribute_name1 = "config"
    # sub_attribute_name2 = "terminal_settings"
    device_name = device_settings.get("device_name")
    device_healthy_period = device_settings.get("device_healthy_period")
    if device_healthy_period is not None:
        device_healthy_period = Decimal(device_healthy_period)

    device_name_key, device_healthy_period_key = ("device_name", "device_healthy_period")
    update_expression = "SET #map.#sub1.#device_name_key = :device_name,\
                        #map.#sub1.#device_healthy_period_key = :device_healthy_period"
    expression_attribute_values = {
        ":device_name": device_name,
        ":device_healthy_period": device_healthy_period,
    }
    expression_attribute_name = {
        "#map": map_attribute_name,
        "#sub1": sub_attribute_name1,
        # "#sub2": sub_attribute_name2,
        "#device_name_key": device_name_key,
        "#device_healthy_period_key": device_healthy_period_key,
    }
    table.update_item(
        Key={"device_id": device_id, "imei": imei},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )
    return
