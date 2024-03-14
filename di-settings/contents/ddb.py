from decimal import Decimal

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
    )
    return response

# デバイス設定更新
def update_device_settings(device_id, imei, device_settings, table):
    map_attribute_name = "device_data"
    sub_attribute_name1 = "config"
    sub_attribute_name2 = "terminal_settings"
    di_new_val = device_settings.get("di_list", {})

    for di in di_new_val:
        di_no = di.get("di_no")
        if di is not None:
            di["di_no"] = Decimal(di_no)

        di_healthy_period = di.get("di_healthy_period")
        if di_healthy_period is not None:
            di["di_healthy_period"] = Decimal(di_healthy_period)

    di_key = "di_list"
    update_expression = "SET #map.#sub1.#sub2.#di_key = :di_new_val"
    expression_attribute_values = {
        ":di_new_val": di_new_val
    }
    expression_attribute_name = {
        "#map": map_attribute_name,
        "#sub1": sub_attribute_name1,
        "#sub2": sub_attribute_name2,
        "#di_key": di_key,
    }
    table.update_item(
        Key={"device_id": device_id, "imei": imei},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )
    return