import uuid

from aws_lambda_powertools import Logger

import cognito
import convert
import db

logger = Logger()


def create_user_info(
    request_params,
    contract_id,
    contract,
    account_table,
    account_table_name,
    user_table_name,
    contract_table_name,
    device_relation_table_name,
):
    # トランザクション書き込み用オブジェクト
    transact_items = []

    #################################################
    # アカウント管理テーブル、Cognito UserPool
    #################################################
    # メールアドレスで検索して、なければ作成、あれば更新
    account = db.get_account_info_by_email_address(request_params["email_address"], account_table)
    if account is None:
        # なければCogitoユーザーを作成し、アカウント管理テーブルに登録
        auth_id = cognito.create_cognito_user(request_params["email_address"])

        account_id = str(uuid.uuid4())
        account_item = {
            "account_id": account_id,
            "email_address": request_params["email_address"],
            "auth_id": auth_id,
            "user_data": {
                "config": {
                    "user_name": request_params["user_name"],
                    "auth_status": "unauthenticated",
                    "password_update_datetime": 0,
                    "mfa_flag": request_params["mfa_flag"],
                }
            },
        }
        account_item_fmt = convert.dict_dynamo_format(account_item)
        put_group = {
            "Put": {
                "TableName": account_table_name,
                "Item": account_item_fmt,
            }
        }
        transact_items.append(put_group)
    else:
        # すでにアカウントがあれば更新
        account_id = account["account_id"]
        if (
            request_params["user_name"] != account["user_data"]["config"]["user_name"]
            or request_params["mfa_flag"] != account["user_data"]["config"]["mfa_flag"]
        ):
            if request_params["mfa_flag"] == 0:
                cognito.clear_cognito_mfa(account["auth_id"])

            account_update_expression = "SET #email_address_attr = :email_address, #map.#config_attr.#user_name_attr = :user_name, #map.#config_attr.#mfa_flag_attr = :mfa_flag"
            account_expression_attribute_values = {
                ":email_address": request_params["email_address"],
                ":user_name": request_params["user_name"],
                ":mfa_flag": request_params["mfa_flag"],
            }
            account_expression_attribute_name = {
                "#email_address_attr": "email_address",
                "#map": "user_data",
                "#config_attr": "config",
                "#user_name_attr": "user_name",
                "#mfa_flag_attr": "mfa_flag",
            }
            account_expression_attribute_values_fmt = convert.dict_dynamo_format(
                account_expression_attribute_values
            )

            update_account = {
                "Update": {
                    "TableName": account_table_name,
                    "Key": {"account_id": {"S": account_id}},
                    "UpdateExpression": account_update_expression,
                    "ExpressionAttributeValues": account_expression_attribute_values_fmt,
                    "ExpressionAttributeNames": account_expression_attribute_name,
                }
            }
            transact_items.append(update_account)

    #################################################
    # モノセコムユーザ管理テーブル
    #################################################
    user_id = str(uuid.uuid4())
    user_item = {
        "user_id": user_id,
        "account_id": account_id,
        "contract_id": contract_id,
        "user_type": request_params["user_type"],
        "user_data": {"config": {"device_order": []}},
    }
    user_item_fmt = convert.dict_dynamo_format(user_item)
    put_user = {
        "Put": {
            "TableName": user_table_name,
            "Item": user_item_fmt,
        }
    }
    transact_items.append(put_user)

    #################################################
    # 契約管理テーブル
    #################################################
    contract_user_list = contract.get("contract_data", {}).get("user_list", {})
    contract_user_list.append(user_id)
    contract_update_expression = f"SET #map.#user_list_attr = :user_list"
    contract_expression_attribute_values = {":user_list": contract_user_list}
    contract_expression_attribute_name = {
        "#map": "contract_data",
        "#user_list_attr": "user_list",
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
    # デバイス関係テーブル（グループ）
    #################################################
    for group_id in request_params["management_group_list"]:
        group_relation_item = {
            "key1": "u-" + user_id,
            "key2": "g-" + group_id,
        }
        group_relation_item_fmt = convert.dict_dynamo_format(group_relation_item)
        put_group_relation = {
            "Put": {
                "TableName": device_relation_table_name,
                "Item": group_relation_item_fmt,
            }
        }
        transact_items.append(put_group_relation)

    #################################################
    # デバイス関係テーブル（デバイス）
    #################################################
    for device_id in request_params["management_device_list"]:
        device_relation_item = {
            "key1": "u-" + user_id,
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

    return transact_result, user_id


def update_user_info(
    request_params,
    account_table,
    user_table,
    device_relation_table,
    account_table_name,
    user_table_name,
    device_relation_table_name,
):
    # トランザクション書き込み用オブジェクト
    transact_items = []

    user_id = request_params["update_user_id"]

    #################################################
    # アカウント管理テーブル、Cognito UserPool
    #################################################
    user = db.get_user_info_by_user_id(user_id, user_table)
    account = db.get_account_info_by_account_id(user["account_id"], account_table)
    if request_params["user_name"] != account.get("user_data", {}).get("config", {}).get(
        "user_name"
    ) or request_params["mfa_flag"] != account.get("user_data", {}).get("config", {}).get(
        "mfa_flag"
    ):
        account_update_expression = f"SET #map.#config_attr.#user_name_attr = :user_name, #map.#config_attr.#mfa_flag_attr = :mfa_flag"
        account_expression_attribute_values = {
            ":user_name": request_params["user_name"],
            ":mfa_flag": request_params["mfa_flag"],
        }
        account_expression_attribute_name = {
            "#map": "user_data",
            "#config_attr": "config",
            "#user_name_attr": "user_name",
            "#mfa_flag_attr": "mfa_flag",
        }
        account_expression_attribute_values_fmt = convert.dict_dynamo_format(
            account_expression_attribute_values
        )

        update_account = {
            "Update": {
                "TableName": account_table_name,
                "Key": {"account_id": {"S": account["account_id"]}},
                "UpdateExpression": account_update_expression,
                "ExpressionAttributeValues": account_expression_attribute_values_fmt,
                "ExpressionAttributeNames": account_expression_attribute_name,
            }
        }
        transact_items.append(update_account)

    #################################################
    # モノセコムユーザ管理テーブル
    #################################################
    if request_params["user_type"] != user.get("user_type"):
        user_update_expression = f"SET #user_type_attr = :user_type"
        user_expression_attribute_values = {
            ":user_type": request_params["user_type"],
        }
        user_expression_attribute_name = {
            "#user_type_attr": "user_type",
        }
        user_expression_attribute_values_fmt = convert.dict_dynamo_format(
            user_expression_attribute_values
        )

        update_user = {
            "Update": {
                "TableName": user_table_name,
                "Key": {"user_id": {"S": user_id}},
                "UpdateExpression": user_update_expression,
                "ExpressionAttributeValues": user_expression_attribute_values_fmt,
                "ExpressionAttributeNames": user_expression_attribute_name,
            }
        }
        transact_items.append(update_user)

    #################################################
    # デバイス関係テーブル（グループ）
    #################################################
    group_relation_list = db.get_device_relation(
        "u-" + user_id, device_relation_table, sk_prefix="g-"
    )
    group_list_old = [relation["key2"][2:] for relation in group_relation_list]

    # 削除されたグループ
    removed_group_list = set(group_list_old) - set(request_params["management_group_list"])
    for remove_group_id in removed_group_list:
        remove_group = {
            "Delete": {
                "TableName": device_relation_table_name,
                "Key": {
                    "key1": {"S": "u-" + user_id},
                    "key2": {"S": "g-" + remove_group_id},
                },
            }
        }
        transact_items.append(remove_group)
    # 追加されたグループ
    added_group_list = set(request_params["management_group_list"]) - set(group_list_old)
    for add_group_id in added_group_list:
        group_relation_item = {
            "key1": "u-" + user_id,
            "key2": "g-" + add_group_id,
        }
        group_relation_item_fmt = convert.dict_dynamo_format(group_relation_item)
        add_group = {
            "Put": {
                "TableName": device_relation_table_name,
                "Item": group_relation_item_fmt,
            }
        }
        transact_items.append(add_group)

    #################################################
    # デバイス関係テーブル（デバイス）
    #################################################
    device_relation_list = db.get_device_relation(
        "u-" + user_id, device_relation_table, sk_prefix="d-"
    )
    device_list_old = [relation["key2"][2:] for relation in device_relation_list]

    # 削除されたデバイス
    removed_device_list = set(device_list_old) - set(request_params["management_device_list"])
    for remove_device_id in removed_device_list:
        remove_device = {
            "Delete": {
                "TableName": device_relation_table_name,
                "Key": {
                    "key1": {"S": "u-" + user_id},
                    "key2": {"S": "d-" + remove_device_id},
                },
            }
        }
        transact_items.append(remove_device)
    # 追加されたデバイス
    added_device_list = set(request_params["management_device_list"]) - set(device_list_old)
    for add_device_id in added_device_list:
        device_relation_item = {
            "key1": "u-" + user_id,
            "key2": "d-" + add_device_id,
        }
        device_relation_item_fmt = convert.dict_dynamo_format(device_relation_item)
        add_device = {
            "Put": {
                "TableName": device_relation_table_name,
                "Item": device_relation_item_fmt,
            }
        }
        transact_items.append(add_device)

    logger.info("------------transact_items-----------")
    logger.info(transact_items)
    logger.info("-------------------------------------")

    # 更新対象なし
    if not transact_items:
        return True, user_id

    #################################################
    # DB書き込みトランザクション実行
    #################################################
    transact_result = db.execute_transact_write_item(transact_items)

    return transact_result, user_id
