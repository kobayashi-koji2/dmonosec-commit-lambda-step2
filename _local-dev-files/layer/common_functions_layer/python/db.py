import json
import os
import traceback
from operator import itemgetter

import boto3
from boto3.dynamodb.conditions import Attr, Key
from aws_lambda_powertools import Logger

logger = Logger()

dynamodb = boto3.resource("dynamodb")
client = boto3.client(
    "dynamodb",
    region_name="ap-northeast-1",
    endpoint_url=os.environ.get("endpoint_url"),
)


###################################
# デバイス関係テーブル取得
#
# **kwargs :
# - gsi_name(任意) : GSI名
# - sk_prefix(任意) : 識別子
#
# 識別子:
# - ユーザID :'u-'
# - デバイスID :'d-'
# - グループID :'g-'
###################################
def get_device_relation(pk, table, **kwargs):
    print(f"pk:{pk}")
    if "gsi_name" in kwargs:
        # GSI PK & SK(識別子の前方一致検索)
        if "sk_prefix" in kwargs:
            response = table.query(
                IndexName=kwargs["gsi_name"],
                KeyConditionExpression=Key("key2").eq(pk)
                & Key("key1").begins_with(kwargs["sk_prefix"]),
            ).get("Items", [])
        # GSI PK検索
        else:
            response = table.query(
                IndexName=kwargs["gsi_name"], KeyConditionExpression=Key("key2").eq(pk)
            ).get("Items", [])
    else:
        # PK & SK(識別子の前方一致検索)
        if "sk_prefix" in kwargs:
            response = table.query(
                KeyConditionExpression=Key("key1").eq(pk)
                & Key("key2").begins_with(kwargs["sk_prefix"])
            ).get("Items", [])

        # PK検索
        else:
            response = table.query(KeyConditionExpression=Key("key1").eq(pk)).get("Items", [])

    return response


# デバイスに紐づくグループID一覧を取得
def get_device_relation_group_id_list(device_id, device_relation_table):
    device_relation_group_list = get_device_relation(
        f"d-{device_id}",
        device_relation_table,
        sk_prefix="g-",
        gsi_name="key2_index",
    )
    group_id_list = [relation["key1"][2:] for relation in device_relation_group_list]
    return group_id_list


# グループに紐づくデバイスID一覧を取得
def get_group_relation_device_id_list(group_id, device_relation_table):
    group_relation_device_list = get_device_relation(
        f"g-{group_id}", device_relation_table, sk_prefix="d-"
    )
    device_id_list = [relation["key2"][2:] for relation in group_relation_device_list]
    return device_id_list


# ユーザーに紐づくグループID一覧を取得
def get_user_relation_group_id_list(user_id, device_relation_table):
    user_relation_group_list = get_device_relation(
        f"u-{user_id}", device_relation_table, sk_prefix="g-"
    )
    group_id_list = [relation["key2"][2:] for relation in user_relation_group_list]
    return group_id_list


# ユーザーに紐づくデバイスID一覧を取得
def get_user_relation_device_id_list(user_id, device_relation_table, include_group_relation=True):
    device_relation_list = get_device_relation(f"u-{user_id}", device_relation_table)
    device_id_list = []
    for relation in device_relation_list:
        relation_id = relation["key2"]
        if relation_id.startswith("d-"):
            device_id_list.append(relation_id[2:])
        elif relation_id.startswith("g-"):
            if include_group_relation:
                device_id_list.extend(
                    [
                        relation_device_id["key2"][2:]
                        for relation_device_id in get_device_relation(
                            relation_id, device_relation_table, sk_prefix="d-"
                        )
                    ]
                )
    return list(set(device_id_list))


# ユーザーに紐づく重複を含むデバイスID一覧を取得
def get_user_relation_duplication_device_id_list(user_id, device_relation_table, include_group_relation=True):
    device_relation_list = get_device_relation(f"u-{user_id}", device_relation_table)
    device_id_list = []
    for relation in device_relation_list:
        relation_id = relation["key2"]
        if relation_id.startswith("d-"):
            device_id_list.append(relation_id[2:])
        elif relation_id.startswith("g-"):
            if include_group_relation:
                device_id_list.extend(
                    [
                        relation_device_id["key2"][2:]
                        for relation_device_id in get_device_relation(
                            relation_id, device_relation_table, sk_prefix="d-"
                        )
                    ]
                )
    return device_id_list


# デバイスに紐づくユーザーID一覧を取得
def get_device_relation_user_id_list(
    device_id, device_relation_table, include_group_relation=True
):
    device_relation_list = get_device_relation(
        f"d-{device_id}", device_relation_table, gsi_name="key2_index"
    )

    user_id_list = []
    for relation in device_relation_list:
        relation_id = relation["key1"]
        if relation_id.startswith("u-"):
            user_id_list.append(relation_id[2:])
        elif relation_id.startswith("g-"):
            if include_group_relation:
                user_id_list.extend(
                    [
                        relation_user_id["key1"][2:]
                        for relation_user_id in get_device_relation(
                            relation_id,
                            device_relation_table,
                            gsi_name="key2_index",
                            sk_prefix="u-",
                        )
                    ]
                )
    return list(set(user_id_list))


# グループに紐づくユーザーID一覧を取得
def get_group_relation_user_id_list(group_id, device_relation_table):
    device_relation_group_list = get_device_relation(
        f"g-{group_id}",
        device_relation_table,
        sk_prefix="u-",
        gsi_name="key2_index",
    )
    user_id_list = [relation["key1"][2:] for relation in device_relation_group_list]
    return user_id_list


# トランザクション(書き込み)
def execute_transact_write_item(transact_items):
    try:
        client.transact_write_items(TransactItems=transact_items)
        return True
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        return False


# モノセコムユーザ情報取得
def get_user_info(pk, sk, table):
    user_info = table.query(
        IndexName="account_id_index",
        KeyConditionExpression=Key("account_id").eq(pk) & Key("contract_id").eq(sk),
    ).get("Items", [])
    # del_datetimeの項目が存在しないか、値がNullの情報（削除済み以外のユーザー情報）のみを取得
    response = [
        item
        for item in user_info
        if item.get("user_data", {}).get("config", {}).get("del_datetime") is None
    ]
    return response


def get_user_info_by_user_id(user_id, table):
    response = table.get_item(Key={"user_id": user_id}).get("Item", {})
    # del_datetimeの項目が存在しないか、値がNullの情報（削除済み以外のユーザー情報）のみを取得
    if response.get("user_data", {}).get("config", {}).get("del_datetime") is not None:
        return {}
    return response


def get_admin_user_id_list(contract_id, table):
    user_info_list = table.query(
        IndexName="contract_id_index",
        KeyConditionExpression=Key("contract_id").eq(contract_id),
        FilterExpression=Attr("user_type").contains("admin"),
    ).get("Items", [])
    user_id_list = [
        user_info["user_id"]
        for user_info in user_info_list
        if user_info.get("user_data", {}).get("config", {}).get("del_datetime") is None
    ]
    return user_id_list


# アカウント情報取得
def get_account_info(pk, table):
    account_info = table.query(
        IndexName="auth_id_index", KeyConditionExpression=Key("auth_id").eq(pk)
    ).get("Items", [])
    # del_datetimeの項目が存在しないか、値がNullの情報（削除済み以外のアカウント情報）のみを取得
    response = [
        item
        for item in account_info
        if item.get("user_data", {}).get("config", {}).get("del_datetime") is None
    ]
    return response[0] if response else None


def get_account_info_by_email_address(email_address, table):
    account_info = table.query(
        IndexName="email_address_index",
        KeyConditionExpression=Key("email_address").eq(email_address),
    ).get("Items", [])
    # del_datetimeの項目が存在しないか、値がNullの情報（削除済み以外のアカウント情報）のみを取得
    response = [
        item
        for item in account_info
        if item.get("user_data", {}).get("config", {}).get("del_datetime") is None
    ]
    return response[0] if response else None


def get_account_info_by_account_id(account_id, table):
    response = table.get_item(Key={"account_id": account_id}).get("Item", {})
    # del_datetimeの項目が存在しないか、値がNullの情報（削除済み以外のアカウント情報）のみを取得
    if response.get("user_data", {}).get("config", {}).get("del_datetime") is not None:
        return {}
    return response


# 契約情報取得
def get_contract_info(contract_id, contract_table):
    contract_info = contract_table.get_item(Key={"contract_id": contract_id}).get("Item", {})
    return contract_info


# デバイス情報取得
def get_device_info(device_id, device_table, consistent_read=False):
    device_list = device_table.query(
        KeyConditionExpression=Key("device_id").eq(device_id),
        FilterExpression=Attr("contract_state").eq(1),
        ConsistentRead=consistent_read,
    ).get("Items", [])
    return insert_id_key_in_device_info(device_list[0]) if device_list else None


# デバイス情報取得(契約状態:使用不可以外)
def get_device_info_other_than_unavailable(device_id, table):
    device_list = table.query(
        KeyConditionExpression=Key("device_id").eq(device_id),
        FilterExpression=Attr("contract_state").ne(2),
    ).get("Items", [])
    return insert_id_key_in_device_info(device_list[0]) if device_list else None


# 現状態取得
def get_device_state(device_id, device_state_table, consistent_read=False):
    device_state = device_state_table.get_item(
        Key={"device_id": device_id},
        ConsistentRead=consistent_read,
    ).get("Item", {})
    return device_state


# 未登録デバイス取得
def get_pre_reg_device_info(contract_id_list, pre_register_table):
    pre_reg_device_list = []
    for item in contract_id_list:
        pre_register_table_res = pre_register_table.query(
            IndexName="contract_id_index",
            KeyConditionExpression=Key("contract_id").eq(item),
        ).get("Items", [])
        pre_register_table_res = insert_id_key_in_device_info_list(pre_register_table_res)
        for items in pre_register_table_res:
            # レスポンス生成(未登録デバイス)
            pre_reg_device_list.append(
                {
                    "imei": items["imei"],
                    "device_registration_datetiime": items["dev_reg_date"],
                }
            )

    return sorted(pre_reg_device_list, key=itemgetter("device_registration_datetiime"))


# グループ情報取得
def get_group_info(group_id, group_table):
    group_info = group_table.get_item(Key={"group_id": group_id}).get("Item", {})
    return group_info


# デバイス順序更新
def update_device_order(device_order, user_id, user_table):
    user_data_attr = "user_data"
    config_attr = "config"
    key = "device_order"
    new_value = device_order
    update_expression = f"SET #user_data_attr.#config_attr.#key = :new_value"
    expression_attribute_values = {":new_value": new_value}
    expression_attribute_name = {
        "#user_data_attr": user_data_attr,
        "#config_attr": config_attr,
        "#key": key,
    }
    user_table.update_item(
        Key={"user_id": user_id},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )
    return ""


def get_remote_control(device_req_no, remote_control_table):
    remote_control_list = remote_control_table.query(
        KeyConditionExpression=Key("device_req_no").eq(device_req_no),
        ScanIndexForward=False,
    ).get("Items", [])
    return remote_control_list[0] if remote_control_list else None

def get_group_relation_pre_register_device_id_list(group_id, device_relation_table):
    group_relation_device_list = get_device_relation(
        f"g-{group_id}", device_relation_table, sk_prefix="pd-"
    )
    device_id_list = [relation["key2"][3:] for relation in group_relation_device_list]
    return device_id_list


def get_device_info_by_imei(pre_register_device_id, pre_register_table):
    device_list = pre_register_table.query(
        KeyConditionExpression=Key("identification_id").eq(pre_register_device_id),
        FilterExpression=Attr("contract_state").ne(2),
    ).get("Items", [])
    return insert_id_key_in_device_info(device_list[0]) if device_list else None

def insert_id_key_in_device_info(info):
    if info.get("device_type") == "UnaTag":
        info["sigfox_id"] = info.get("identification_id")
        info["imei"] = ""
    else:
        info["sigfox_id"] = ""
        info["imei"] = info.get("identification_id")
    return info

def insert_id_key_in_device_info_list(info_list):
    device_info_list = []
    for item in info_list:
        if item.get("device_type") == "UnaTag":
            item["sigfox_id"] = item.get("identification_id")
            item["imei"] = ""
        else:
            item["sigfox_id"] = ""
            item["imei"] = item.get("identification_id")
        device_info_list.append(item)
    return device_info_list