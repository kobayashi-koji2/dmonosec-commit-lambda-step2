from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

import db

logger = Logger()
dynamodb = boto3.resource("dynamodb")


# タイマー設定削除
def delete_timer_settings(device_id, do_no, do_timer_id, device_table):

    # 現在のタイマー設定を取得し、条件に一致するものをリストから削除
    device = db.get_device_info_other_than_unavailable(device_id, device_table)
    do_list = (
        device.get("device_data", {})
        .get("config", {})
        .get("terminal_settings", {})
        .get("do_list", [])
    )
    logger.debug(f"before do_list: {do_list}")
    for do in do_list:
        if do.get("do_no") == do_no:
            do_timer_list = do.get("do_timer_list", [])
            for do_timer in do_timer_list:
                if do_timer.get("do_timer_id") == do_timer_id:
                    do_timer_list.remove(do_timer)
                    break
    logger.debug(f"before delete: {do_list}")

    # テーブル更新
    update_expression = "SET #map.#sub1.#sub2.#sub3 = :do_list"
    expression_attribute_values = {":do_list": do_list}
    expression_attribute_name = {
        "#map": "device_data",
        "#sub1": "config",
        "#sub2": "terminal_settings",
        "#sub3": "do_list",
    }
    device_table.update_item(
        Key={"device_id": device_id, "imei": device.get("imei")},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )
    return True
