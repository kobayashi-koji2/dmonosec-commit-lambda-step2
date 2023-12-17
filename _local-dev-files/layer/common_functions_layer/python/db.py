import os
import json

import boto3
from boto3.dynamodb.conditions import Key, Attr
from operator import itemgetter


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
            ).get("Items", {})
        # GSI PK検索
        else:
            response = table.query(
                IndexName=kwargs["gsi_name"], KeyConditionExpression=Key("key2").eq(pk)
            ).get("Items", {})
    else:
        # PK & SK(識別子の前方一致検索)
        if "sk_prefix" in kwargs:
            response = table.query(
                KeyConditionExpression=Key("key1").eq(pk)
                & Key("key2").begins_with(kwargs["sk_prefix"])
            ).get("Items", {})

        # PK検索
        else:
            response = table.query(KeyConditionExpression=Key("key1").eq(pk)).get(
                "Items", {}
            )

    return response


# トランザクション(書き込み)
def execute_transact_write_item(transact_items):
    try:
        client.transact_write_items(TransactItems=transact_items)
        return True
    except Exception as e:
        print(e)
        return False


# モノセコムユーザ情報取得
def get_user_info(pk, sk, table):
    response = table.query(
        IndexName="account_id_index",
        KeyConditionExpression=Key("account_id").eq(pk) & Key("contract_id").eq(sk),
    )
    return response


def get_user_info_by_user_id(user_id, table):
    response = table.get_item(Key={"user_id": user_id})
    return response


# アカウント情報取得
def get_account_info(pk, table):
    response = table.query(
        IndexName="auth_id_index", KeyConditionExpression=Key("auth_id").eq(pk)
    )
    return response


# 契約情報取得
def get_contract_info(contract_id, contract_table):
    contract_info = contract_table.get_item(Key={"contract_id": contract_id})
    return contract_info


# デバイス情報取得
def get_device_info(device_id, device_table):
    device_list = device_table.query(
        KeyConditionExpression=Key("device_id").eq(device_id),
        FilterExpression=Attr("contract_state").eq(1),
    ).get("Items")
    return device_list[0] if device_list else None


# 現状態取得
def get_device_state(device_id, device_state_table):
    device_state = device_state_table.get_item(Key={"device_id": device_id})
    return device_state


# 未登録デバイス取得
def get_pre_reg_device_info(contract_id_list, pre_register_table):
    pre_reg_device_list = []
    for item in contract_id_list:
        pre_register_table_res = pre_register_table.query(
            IndexName="contract_id_index",
            KeyConditionExpression=Key("contract_id").eq(item),
        ).get("Items", [])
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
    group_info = group_table.get_item(Key={"group_id": group_id})
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
