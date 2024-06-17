from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

import db

logger = Logger()
dynamodb = boto3.resource("dynamodb")


# デバイス設定更新
def update_device_settings(device_id, params, device_table, automation_table):
    map_attribute_name = "device_data"
    sub_attribute_name1 = "config"
    sub_attribute_name2 = "terminal_settings"
    param_do_list = params.get("do_list", {})

    device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
    do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
    update_control_list = []
    for do in do_list:
        for param_do in param_do_list:
            if do["do_no"] == param_do.get("do_no"):
                # コントロール設定が変更されている場合はスケジュール設定をクリア
                if (
                    do["do_control"] != param_do.get("do_control")
                    or do["do_specified_time"] != param_do.get("do_specified_time")
                    or do["do_di_return"] != param_do.get("do_di_return")
                ):
                    do["do_timer_list"] = []
                    update_control_list.append(do["do_no"])
                do["do_control"] = param_do.get("do_control")
                do["do_di_return"] = param_do.get("do_di_return") or 0
                do["do_name"] = param_do.get("do_name")
                do["do_specified_time"] = param_do.get("do_specified_time")
                break

    do_key = "do_list"
    update_expression = "SET #map.#sub1.#sub2.#do_key = :do_new_val"

    expression_attribute_values = {
        ":do_new_val": do_list,
    }
    expression_attribute_name = {
        "#map": map_attribute_name,
        "#sub1": sub_attribute_name1,
        "#sub2": sub_attribute_name2,
        "#do_key": do_key,
    }
    device_table.update_item(
        Key={"device_id": device_id, "imei": device_info.get("imei")},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )

    if update_control_list:
        for do_no in update_control_list:
            # コントロール設定が変更されている場合は対象のオートメーション設定クリア
            automation_list = automation_table.query(
                IndexName="control_device_id_index",
                KeyConditionExpression=Key("control_device_id").eq(device_id),
            ).get("Items", [])
            for automation in automation_list:
                if automation.get("control_do_no") == do_no:
                    automation_table.delete_item(
                        Key={"automation_id": automation.get("automation_id")}
                    )

    return
