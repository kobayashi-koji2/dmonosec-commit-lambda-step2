import uuid

from aws_lambda_powertools import Logger

import db
import convert


logger = Logger()


def generate_group_id():
    return str(uuid.uuid4()).replace("-", "")


def create_group_info(
    group_info,
    contract,
    group_table_name,
    contract_table_name,
    device_relation_table_name,
):
    # グループID生成
    group_id = generate_group_id()
    # トランザクション書き込み用オブジェクト
    transact_items = []
    # テーブル更新用キー
    contract_data_attr = "contract_data"
    group_list_attr = "group_list"
    # グループ名
    group_name = group_info.get("group_name")
    # グループに登録するデバイス
    device_list = group_info.get("device_list", {})

    #################################################
    # グループ管理テーブル新規登録用オブジェクト作成
    #################################################
    item = {
        "group_id": group_id,
        "group_data": {
            "config": {
                "contract_id": contract.get("contract_id"),
                "group_name": group_name,
            }
        },
    }
    item_fmt = convert.dict_dynamo_format(item)
    put_group = {
        "Put": {
            "TableName": group_table_name,
            "Item": item_fmt,
        }
    }
    transact_items.append(put_group)

    #################################################
    # 契約管理テーブル更新用オブジェクト作成
    #################################################
    contract_group_list = contract.get("contract_data", {}).get("group_list", {})
    contract_group_list.append(group_id)
    contract_update_expression = f"SET #map.#group_list_attr = :group_list"
    contract_expression_attribute_values = {":group_list": contract_group_list}
    contract_expression_attribute_name = {
        "#map": contract_data_attr,
        "#group_list_attr": group_list_attr,
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
    # デバイス関係テーブル新規登録用オブジェクト作成
    #################################################
    for device_id in device_list:
        device_relation_item = {
            "key1": "g-" + group_id,
            "key2": "d-" + device_id,
        }
        device_relation_item_fmt = convert.dict_dynamo_format(device_relation_item)
        put_device_relation = {
            "Put": {
                "TableName": device_relation_table_name,
                "Item": device_relation_item_fmt,
            }
        }
        transact_items.append(put_device_relation)

    #################################################
    # DB書き込みトランザクション実行
    #################################################
    transact_result = db.execute_transact_write_item(transact_items)

    return transact_result, group_id


def update_group_info(
    group_info,
    group_id,
    device_relation_table,
    group_table_name,
    device_relation_table_name,
):
    # トランザクション書き込み用オブジェクト
    transact_items = []
    # テーブル更新用キー
    group_data_attr = "group_data"
    config_attr = "config"
    group_name_attr = "group_name"
    group_name = group_info.get("group_name", "")
    device_list = group_info.get("device_list", {})

    #################################################
    # グループ管理テーブル更新用オブジェクト作成
    #################################################
    group_update_expression = f"SET #group_data.#config.#group_name = :group_name"
    group_expression_attribute_values = {":group_name": group_name}
    group_expression_attribute_name = {
        "#group_data": group_data_attr,
        "#config": config_attr,
        "#group_name": group_name_attr,
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
    # デバイス管理テーブル更新用オブジェクト作成
    #################################################
    # グループ更新前のデバイス一覧
    device_list_old = db.get_group_relation_device_id_list(group_id, device_relation_table)

    # グループから削除されたデバイス
    removed_devices = set(device_list_old) - set(device_list)
    for remove_device_id in removed_devices:
        remove_device = {
            "Delete": {
                "TableName": device_relation_table_name,
                "Key": {
                    "key1": {"S": "g-" + group_id},
                    "key2": {"S": "d-" + remove_device_id},
                },
            }
        }
        logger.info(remove_device)
        transact_items.append(remove_device)

    # グループに追加されたデバイス
    added_devices = set(device_list) - set(device_list_old)
    for add_device_id in added_devices:
        group_relation_item = {
            "key1": "g-" + group_id,
            "key2": "d-" + add_device_id,
        }
        group_relation_item_fmt = convert.dict_dynamo_format(group_relation_item)
        add_device = {
            "Put": {
                "TableName": device_relation_table_name,
                "Item": group_relation_item_fmt,
            }
        }
        transact_items.append(add_device)

    #################################################
    # DB書き込みトランザクション実行
    #################################################
    transact_result = db.execute_transact_write_item(transact_items)

    return transact_result, group_id
