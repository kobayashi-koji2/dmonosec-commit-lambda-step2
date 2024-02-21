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
    device_name = device_settings.get("device_name")
    device_healthy_period = device_settings.get("device_healthy_period")
    if device_healthy_period is not None:
        device_healthy_period = Decimal(device_healthy_period)
    di_new_val = device_settings.get("di_list", {})
    do_new_val = device_settings.get("do_list", {})
    ai_new_val = device_settings.get("ai_list", {})
    for di in di_new_val:
        di_no = di.get("di_no")
        if di is not None:
            di["di_no"] = Decimal(di_no)

        di_healthy_period = di.get("di_healthy_period")
        if di_healthy_period is not None:
            di["di_healthy_period"] = Decimal(di_healthy_period)

    for do in do_new_val:
        do_no = do.get("do_no")
        if do is not None:
            do["do_no"] = Decimal(do_no)

        do_specified_time = do.get("do_specified_time")
        if do_specified_time is not None:
            do["do_specified_time"] = Decimal(do_specified_time)

        do_di_return = do.get("do_di_return")
        if do_di_return is not None:
            do["do_di_return"] = Decimal(do_di_return)

        do_timer_list = do.get("do_timer_list", [])
        for do_timer in do_timer_list:
            do_onoff_control = do_timer.get("do_onoff_control")
            if do_onoff_control is not None:
                do_timer["do_onoff_control"] = Decimal(do_onoff_control)

    for ai in ai_new_val:
        ai_no = ai.get("ai_no")
        if ai_no is not None:
            ai["ai_no"] = Decimal(ai_no)

    di_key, do_key, ai_key, device_name_key, device_healthy_period_key = "di_list", "do_list", "ai_list", "device_name", "device_healthy_period"
    update_expression = "SET #map.#sub1.#device_name_key = :device_name,\
                        #map.#sub1.#device_healthy_period_key = :device_healthy_period,\
                        #map.#sub1.#sub2.#di_key = :di_new_val,\
                        #map.#sub1.#sub2.#do_key = :do_new_val,\
                        #map.#sub1.#sub2.#ai_key = :ai_new_val"
    expression_attribute_values = {
        ":di_new_val": di_new_val,
        ":do_new_val": do_new_val,
        ":ai_new_val": ai_new_val,
        ":device_name": device_name,
        ":device_healthy_period": device_healthy_period,
    }
    expression_attribute_name = {
        "#map": map_attribute_name,
        "#sub1": sub_attribute_name1,
        "#sub2": sub_attribute_name2,
        "#di_key": di_key,
        "#do_key": do_key,
        "#ai_key": ai_key,
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
