from aws_lambda_powertools import Logger

import db
import ssm
import convert

logger = Logger()


def update_device_notification_settings(notificaton_settings_list, device_table):
    # トランザクション書き込み用オブジェクト
    transact_items = []

    for device_id, notificaton_settings in notificaton_settings_list.items():
        device = db.get_device_info_other_than_unavailable(device_id, device_table)
        notificaton_settings_fmt = convert.to_dynamo_format(notificaton_settings)
        update_device = {
            "Update": {
                "TableName": ssm.table_names["DEVICE_TABLE"],
                "Key": {
                    "device_id": {"S": device["device_id"]},
                    "imei": {"S": device["imei"]},
                },
                "UpdateExpression": "set #map_d.#map_c.#map_n = :s",
                "ExpressionAttributeNames": {
                    "#map_d": "device_data",
                    "#map_c": "config",
                    "#map_n": "notification_settings",
                },
                "ExpressionAttributeValues": {":s": notificaton_settings_fmt},
            }
        }
        transact_items.append(update_device)

    transact_result = db.execute_transact_write_item(transact_items)
    logger.debug(transact_result)
    return transact_result
