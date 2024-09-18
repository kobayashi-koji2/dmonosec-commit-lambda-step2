import time
import db

from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key, Attr

logger = Logger()


# ICCID管理情報取得
def get_iccid_info(sim_id, iccid_table):
    iccid_list = iccid_table.query(KeyConditionExpression=Key("iccid").eq(sim_id)).get("Items")
    return iccid_list[0] if iccid_list else None


# デバイス情報取得
def get_device_info(device_id, contract_state, device_table):
    device_list = device_table.query(
        KeyConditionExpression=Key("device_id").eq(device_id),
        FilterExpression=Attr("contract_state").eq(contract_state),
    ).get("Items")
    return db.add_imei_in_device_info(device_list[0]) if device_list else None


# OPID情報取得
def get_opid_info(operator_table):
    opid_list = operator_table.query(KeyConditionExpression=Key("service").eq("monosc")).get(
        "Items"
    )
    return opid_list[0] if opid_list else None


# 初期受信日時更新
def update_init_recv(device_id, imei, device_table):
    contract_state = 1
    init_datetime = int(time.time() * 1000)

    option = {
        "Key": {
            "device_id": device_id,
            "imei": imei,
        },
        "UpdateExpression": "set #contract_state = :contract_state,\
          #device_data.#param.#init_datetime = :init_datetime",
        "ExpressionAttributeNames": {
            "#contract_state": "contract_state",
            "#device_data": "device_data",
            "#param": "param",
            "#init_datetime": "init_datetime",
        },
        "ExpressionAttributeValues": {":contract_state": contract_state, ":init_datetime": init_datetime},
    }
    device_table.update_item(**option)


# デバイス解約更新
def update_sim_stop(device_id, imei, device_table):
    contract_state = 2
    del_datetime = int(time.time() * 1000)

    option = {
        "Key": {
            "device_id": device_id,
            "imei": imei,
        },
        "UpdateExpression": "set #contract_state = :contract_state,\
          #device_data.#param.#del_datetime = :del_datetime",
        "ExpressionAttributeNames": {
            "#contract_state": "contract_state",
            "#device_data": "device_data",
            "#param": "param",
            "#del_datetime": "del_datetime",
        },
        "ExpressionAttributeValues": {":contract_state": contract_state, ":del_datetime": del_datetime},
    }
    device_table.update_item(**option)
