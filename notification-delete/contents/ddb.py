from aws_lambda_powertools import Logger

import db
import ssm
import convert

logger = Logger()


def delete_device_notification_settings(device_id, device_table):
    # トランザクション書き込み用オブジェクト
    transact_items = []

    device = db.get_device_info_other_than_unavailable(device_id, device_table)
    notificaton_settings = []
    notification_target_list = []
    notificaton_settings_fmt = convert.to_dynamo_format(notificaton_settings)
    notification_target_list_fmt = convert.to_dynamo_format(notification_target_list)
    update_device = {
        "Update": {
            "TableName": ssm.table_names["DEVICE_TABLE"],
            "Key": {
                "device_id": {"S": device["device_id"]},
                "imei": {"S": device["imei"]},
            },
            "UpdateExpression": "set #map_d.#map_c.#map_n = :s, #map_d.#map_c.#map_t = :t",
            "ExpressionAttributeNames": {
                "#map_d": "device_data",
                "#map_c": "config",
                "#map_n": "notification_settings",
                "#map_t": "notification_target_list",
            },
            "ExpressionAttributeValues": {
                ":s": notificaton_settings_fmt,
                ":t": notification_target_list_fmt,
            },
        }
    }
    transact_items.append(update_device)

    transact_result = db.execute_transact_write_item(transact_items)
    logger.debug(transact_result)
    return device
