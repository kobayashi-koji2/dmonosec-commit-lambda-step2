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
def update_device_settings(device_id, imei, timer_settings, table):
    timer_value = {}

    do_no = timer_settings.get("do_no") - 1

    timer_value["do_timer_id"] = timer_settings.get("do_timer_id", "")
    timer_value["do_timer_name"] = timer_settings.get("do_timer_name", "")
    timer_value["do_onoff_control"] = Decimal(timer_settings.get("do_onoff_control"))
    timer_value["do_time"] = timer_settings.get("do_time", "")
    timer_value["do_weekday"] = timer_settings.get("do_weekday", "")
    logger.info(f"timer_value={timer_value}")

    # 接点出力タイマー一覧を取得
    device_info = get_device_info(device_id, table).get("Items", {})
    device_info = device_info[0]
    do_timer_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"][do_no]
    logger.info(f"do_timer_list={do_timer_list}")

    # 接点出力_タイマーIDがなければ、新規作成
    if timer_value["do_timer_id"] == "":
        timer_value["do_timer_id"] = str(uuid.uuid4())
        do_timer_list.append(timer_value)
        logger.info(f"add timer_id={timer_value["do_timer_id"]}")
    else:
        for index, item in enumerate(do_timer_list):
            if item["do_timer_id"] == timer_value["do_timer_id"] :
                do_timer_list[index] = timer_value
                logger.info(f"update timer_id={timer_value["do_timer_id"]}")
                break

    do_key = "do_list"
    update_expression = "SET #map1.#map2.#map3.#list1[#do_no] = :do_list_val"
    expression_attribute_values = {":do_list_val": do_timer_list}
    expression_attribute_name = {
        "#map1": "device_data",
        "#map2": "config",
        "#map3": "terminal_settings",
        "#list1": "do_list",
        "#do_no": do_no
    }
    table.update_item(
        Key={"device_id": device_id, "imei": imei},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )
    return
