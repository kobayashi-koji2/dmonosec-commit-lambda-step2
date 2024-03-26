import uuid
from decimal import Decimal

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

import db

logger = Logger()
dynamodb = boto3.resource("dynamodb")


# デバイス設定更新
def update_device_settings(device_id, timer_settings, table):
    timer_value = {}

    do_no = timer_settings.get("do_no")

    timer_value["do_timer_id"] = timer_settings.get("do_timer_id", "")
    timer_value["do_timer_name"] = timer_settings.get("do_timer_name", "")
    timer_value["do_onoff_control"] = Decimal(timer_settings.get("do_onoff_control"))
    timer_value["do_time"] = timer_settings.get("do_time", "")
    timer_value["do_weekday"] = timer_settings.get("do_weekday", "")
    logger.info(f"timer_value={timer_value}")

    # 接点出力タイマー一覧を取得
    device_info = db.get_device_info_other_than_unavailable(device_id, table)
    do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
    do_timer_list = []
    for do in do_list:
        if do["do_no"] == do_no:
            do_timer_list = do.get("do_timer_list", [])
            break
    logger.info(f"do_timer_list={do_timer_list}")

    # 接点出力_タイマーIDがなければ、新規作成
    if not timer_value["do_timer_id"]:
        timer_value["do_timer_id"] = str(uuid.uuid4())
        do_timer_list.append(timer_value)
        logger.info(f"add timer_id={timer_value['do_timer_id']}")
    else:
        for index, item in enumerate(do_timer_list):
            if item["do_timer_id"] == timer_value["do_timer_id"]:
                do_timer_list[index] = timer_value
                logger.info(f"update timer_id={timer_value['do_timer_id']}")
                break
    logger.debug(f"do_list: {do_list}")

    update_expression = "SET #map.#sub1.#sub2.#sub3 = :do_list"
    expression_attribute_values = {":do_list": do_list}
    expression_attribute_name = {
        "#map": "device_data",
        "#sub1": "config",
        "#sub2": "terminal_settings",
        "#sub3": "do_list",
    }
    table.update_item(
        Key={"device_id": device_id, "imei": device_info.get("imei")},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )
    return
