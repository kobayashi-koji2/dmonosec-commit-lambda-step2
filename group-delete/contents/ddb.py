import time

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key

import db
import convert

dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb", region_name="ap-northeast-1")
logger = Logger()


def delete_group_info(
    group_id,
    contract,
    device_relation_table,
    device_table,
    contract_table_name,
    group_table_name,
    device_relation_table_name,
    device_table_name,
):
    # トランザクション書き込み用オブジェクト
    transact_items = []
    # テーブル更新用キー
    group_data_attr = "group_data"
    config_attr = "config"
    del_datetime_attr = "del_datetime"
    contract_data_attr = "contract_data"
    group_list_attr = "group_list"

    #################################################
    # 契約管理テーブル更新用オブジェクト作成
    #################################################
    group_list = contract.get("contract_data", []).get("group_list", [])
    group_list.remove(group_id)
    contract_update_expression = f"SET #contract_data.#group_list = :group_list"
    contract_expression_attribute_values = {":group_list": group_list}
    contract_expression_attribute_name = {
        "#contract_data": contract_data_attr,
        "#group_list": group_list_attr,
    }
    contract_expression_attribute_values_fmt = convert.dict_dynamo_format(
        contract_expression_attribute_values
    )

    update_contract = {
        "Update": {
            "TableName": contract_table_name,
            "Key": {"contract_id": {"S": contract["contract_id"]}},
            "UpdateExpression": contract_update_expression,
            "ExpressionAttributeValues": contract_expression_attribute_values_fmt,
            "ExpressionAttributeNames": contract_expression_attribute_name,
        }
    }
    transact_items.append(update_contract)

    #################################################
    # デバイス管理テーブル（通知先設定）
    #################################################
    relation_device_id_list = db.get_group_relation_device_id_list(group_id, device_relation_table)
    relation_user_id_list = db.get_group_relation_user_id_list(group_id, device_relation_table)
    for device_id in relation_device_id_list:
        remove_user_id_list = []
        device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
        notification_target_list = device_info.get('device_data', {}).get('config', {}).get('notification_target_list', [])
        user_id_list = list(set(relation_user_id_list) & set(notification_target_list))
        for user_id in user_id_list:
            device_id_list_old = db.get_user_relation_duplication_device_id_list(user_id, device_relation_table)
            if device_id_list_old.count(device_id) <= 1:
                remove_user_id_list.append(user_id)
        if remove_user_id_list:
            notification_target_list = list(set(notification_target_list) - set(remove_user_id_list))
            device_update_expression = f"SET #device_data.#config.#notification_target_list = :notification_target_list"
            device_expression_attribute_values = {
                ":notification_target_list": notification_target_list,
            }
            device_expression_attribute_name = {
                "#device_data": "device_data",
                "#config": "config",
                "#notification_target_list": "notification_target_list",
            }
            device_expression_attribute_values_fmt = convert.dict_dynamo_format(
                device_expression_attribute_values
            )

            update_device = {
                "Update": {
                    "TableName": device_table_name,
                    "Key": {
                        "device_id": {"S": device_info["device_id"]},
                        "imei": {"S": device_info["imei"]}
                    },
                    "UpdateExpression": device_update_expression,
                    "ExpressionAttributeValues": device_expression_attribute_values_fmt,
                    "ExpressionAttributeNames": device_expression_attribute_name,
                }
            }
            transact_items.append(update_device)

    #################################################
    # デバイス関係テーブル削除用オブジェクト作成
    #################################################
    device_relation_list = db.get_device_relation(
        "g-" + group_id, device_relation_table, sk_prefix="d-"
    )
    logger.info(device_relation_list)
    for device_relation in device_relation_list:
        remove_relation = {
            "Delete": {
                "TableName": device_relation_table_name,
                "Key": {
                    "key1": {"S": device_relation["key1"]},
                    "key2": {"S": device_relation["key2"]},
                },
            }
        }
        transact_items.append(remove_relation)

    user_relation_list = db.get_device_relation(
        "g-" + group_id, device_relation_table, sk_prefix="u-", gsi_name="key2_index"
    )
    logger.info(user_relation_list)
    for user_relation in user_relation_list:
        remove_relation = {
            "Delete": {
                "TableName": device_relation_table_name,
                "Key": {
                    "key1": {"S": user_relation["key1"]},
                    "key2": {"S": user_relation["key2"]},
                },
            }
        }
        transact_items.append(remove_relation)

    #################################################
    # グループテーブル更新用オブジェクト作成
    #################################################
    group_update_expression = f"SET #group_data.#config.#del_datetime = :del_datetime"
    group_expression_attribute_values = {":del_datetime": int(time.time() * 1000)}
    group_expression_attribute_name = {
        "#group_data": group_data_attr,
        "#config": config_attr,
        "#del_datetime": del_datetime_attr,
    }
    group_expression_attribute_values_fmt = convert.dict_dynamo_format(
        group_expression_attribute_values
    )

    update_group = {
        "Update": {
            "TableName": group_table_name,
            "Key": {"group_id": {"S": group_id}},
            "UpdateExpression": group_update_expression,
            "ExpressionAttributeValues": group_expression_attribute_values_fmt,
            "ExpressionAttributeNames": group_expression_attribute_name,
        }
    }
    transact_items.append(update_group)

    #################################################
    # DB書き込みトランザクション実行
    #################################################
    return db.execute_transact_write_item(transact_items)
