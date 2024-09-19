import uuid
import os

from datetime import datetime
from dateutil import relativedelta
from aws_lambda_powertools import Logger

import cognito
import convert
import db

logger = Logger()

TEMPORARY_PASSWORD_PERIOD_DAYS = int(os.environ["TEMPORARY_PASSWORD_PERIOD_DAYS"])


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
        auth_period = int(
            (
                datetime.now() + relativedelta.relativedelta(days=TEMPORARY_PASSWORD_PERIOD_DAYS)
            ).timestamp()
            * 1000
        )

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
                    "auth_period": auth_period,
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
                cognito.clear_cognito_mfa(account["email_address"])

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
    if len(transact_items) > 100:
        transact_items_list = [transact_items[i:i+100] for i in range(0, len(transact_items), 100)]
    else:
        transact_items_list = [transact_items]
    logger.info("------------transact_items_list-----------")
    logger.info(transact_items_list)
    logger.info("------------------------------------------")

    for transact_item in transact_items_list:
        transact_result = db.execute_transact_write_item(transact_item)
        if not transact_result:
            break

    return transact_result, user_id


def update_user_info(
    request_params,
    account_table,
    user_table,
    device_relation_table,
    device_table,
    contract_table,
    account_table_name,
    user_table_name,
    device_relation_table_name,
    device_table_name,
):
    # トランザクション書き込み用オブジェクト
    transact_items = []
    remove_device_id_list = []
    added_device_id_list = []

    user_id = request_params["update_user_id"]

    #################################################
    # アカウント管理テーブル、Cognito UserPool
    #################################################
    user = db.get_user_info_by_user_id(user_id, user_table)
    logger.info(user)
    account = db.get_account_info_by_account_id(user["account_id"], account_table)
    if request_params["user_name"] != account.get("user_data", {}).get("config", {}).get(
        "user_name"
    ) or request_params["mfa_flag"] != account.get("user_data", {}).get("config", {}).get(
        "mfa_flag"
    ):
        if request_params["mfa_flag"] == 0:
            cognito.clear_cognito_mfa(account["email_address"])

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
    removed_group_list = convert.list_difference(group_list_old, request_params["management_group_list"])
    for remove_group_id in removed_group_list:
        remove_group_relation_device_id_list = db.get_group_relation_device_id_list(remove_group_id, device_relation_table)
        remove_device_id_list.extend(remove_group_relation_device_id_list)
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
    added_group_list = convert.list_difference(request_params["management_group_list"], group_list_old)
    for add_group_id in added_group_list:
        added_group_relation_device_id_list = db.get_group_relation_device_id_list(add_group_id, device_relation_table)
        added_device_id_list.extend(added_group_relation_device_id_list)
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
    removed_device_list = convert.list_difference(device_list_old, request_params["management_device_list"])
    remove_device_id_list.extend(removed_device_list)
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
    added_device_list = convert.list_difference(request_params["management_device_list"], device_list_old)
    added_device_id_list.extend(added_device_list)
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


    #################################################
    # デバイス管理テーブル（通知先設定）
    #################################################
    logger.info(user)
    if user["user_type"] in ["worker", "referrer"] and request_params["user_type"] in ["worker", "referrer"]:
        device_id_list_old = db.get_user_relation_duplication_device_id_list(user_id, device_relation_table)
        remove_device_id_list = convert.list_difference(remove_device_id_list, added_device_id_list)
        remove_device_id_set = set(remove_device_id_list)
        for device_id in remove_device_id_set:
            if remove_device_id_list.count(device_id) == device_id_list_old.count(device_id):
                device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
                logger.debug(device_info)
                notification_target_list = device_info.get('device_data', {}).get('config', {}).get('notification_target_list', [])
                if user_id in notification_target_list:
                    notification_target_list.remove(user_id)
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
                                "identification_id": {"S": device_info["imei"]}
                            },
                            "UpdateExpression": device_update_expression,
                            "ExpressionAttributeValues": device_expression_attribute_values_fmt,
                            "ExpressionAttributeNames": device_expression_attribute_name,
                        }
                    }
                    transact_items.append(update_device)
    elif user["user_type"] in ["admin", "sub_admin"] and request_params["user_type"] in ["worker", "referrer"]:
        contract_info = db.get_contract_info(user.get("contract_id"), contract_table)
        contract_device_id_list = contract_info.get("contract_data", {}).get("device_list", [])
        admin_remove_device_id_list = set(contract_device_id_list) - set(added_device_id_list)
        for device_id in admin_remove_device_id_list:
            device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
            notification_target_list = device_info.get('device_data', {}).get('config', {}).get('notification_target_list', [])
            if user_id in notification_target_list:
                notification_target_list.remove(user_id)
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
                            "identification_id": {"S": device_info["imei"]}
                        },
                        "UpdateExpression": device_update_expression,
                        "ExpressionAttributeValues": device_expression_attribute_values_fmt,
                        "ExpressionAttributeNames": device_expression_attribute_name,
                    }
                }
                transact_items.append(update_device)

    logger.info("------------transact_items-----------")
    logger.info(transact_items)
    logger.info("-------------------------------------")

    # 更新対象なし
    if not transact_items:
        return True, user_id

    #################################################
    # DB書き込みトランザクション実行
    #################################################
    if len(transact_items) > 100:
        transact_items_list = [transact_items[i:i+100] for i in range(0, len(transact_items), 100)]
    else:
        transact_items_list = [transact_items]
    logger.info("------------transact_items_list-----------")
    logger.info(transact_items_list)
    logger.info("------------------------------------------")

    for transact_item in transact_items_list:
        transact_result = db.execute_transact_write_item(transact_item)
        if not transact_result:
            break

    return transact_result, user_id
