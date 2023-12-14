import json
import os

import boto3
from boto3.dynamodb.conditions import Key
from operator import itemgetter


dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
client = boto3.client(
    "dynamodb",
    region_name="ap-northeast-1",
    endpoint_url=os.environ.get("endpoint_url"),
)


# ユーザ情報取得
def get_user_info(user_id, user_table):
    user_table = user_table.get_item(Key={"user_id": user_id})
    return user_table


# アカウント情報取得
def get_account_info(user_id, account_table):
    account_table = account_table.query(
        IndexName="user_id_index", KeyConditionExpression=Key("user_id").eq(user_id)
    )

    return account_table


# 契約情報取得
def get_contract_info(contract_id, contract_table):
    contract_info = contract_table.get_item(Key={"contract_id": contract_id})
    return contract_info


# デバイス情報取得
def get_device_info(device_id, device_table):
    device_info = device_table.query(
        IndexName="contract_state_index",
        KeyConditionExpression=Key("device_id").eq(device_id)
        & Key("contract_state").eq(1),
    )
    return device_info


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


# トランザクション(書き込み)
def execute_transact_write_item(transact_items):
    try:
        client.transact_write_items(TransactItems=transact_items)
        return True
    except Exception as e:
        print(e)
        return False
