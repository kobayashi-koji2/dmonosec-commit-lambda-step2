import uuid
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
    do_new_val = device_settings.get("do_list", {})
    result = ""

    for do in do_new_val:
        do_no = do.get("do_no")
        if do is not None:
            do["do_no"] = Decimal(do_no)

        do_timer_list = do.get("do_timer_list", [])
        for do_timer in do_timer_list:
            req_do_timer_id = do_timer.get("do_timer_id", "")
            # 接点出力_タイマーIDがなければ、新規作成
            if req_do_timer_id == "":
                do_timer["do_timer_id"] = str(uuid.uuid4())
                result = True
                return result
            # 値がある場合、更新
            else:
                # 接点出力タイマー一覧を取得
                device_info = get_device_info(device_id, table).get("Items", {})
                device_info = device_info[0]
                do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
                for do in do_list:
                    do_timer_list = do.get("do_timer_list", [])
                    for do_timer in do_timer_list:
                        logger.info(f"req_do_timer_id: {req_do_timer_id}")
                        do_timer["do_timer_id"] = req_do_timer_id

            do_onoff_control = do_timer.get("do_onoff_control")
            if do_onoff_control is not None:
                do_timer["do_onoff_control"] = Decimal(do_onoff_control)

    do_key = "do_list"
    update_expression = "SET #map.#sub1.#sub2.#do_key = :do_new_val"
    expression_attribute_values = {":do_new_val": do_new_val}
    expression_attribute_name = {
        "#map": map_attribute_name,
        "#sub1": sub_attribute_name1,
        "#sub2": sub_attribute_name2,
        "#do_key": do_key,
    }
    table.update_item(
        Key={"device_id": device_id, "imei": imei},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )
    return
