from decimal import Decimal
from datetime import datetime
import db
import time

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

logger = Logger()
dynamodb = boto3.resource("dynamodb")


# デバイス情報取得(契約状態:使用不可以外)
def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    ).get("Items", [])
    return db.insert_id_key_in_device_info_list(response)


# デバイス設定更新
def update_device_settings(device_id, imei, device_settings, device_table, device_state_table):
    map_attribute_name = "device_data"
    sub_attribute_name1 = "config"
    sub_attribute_name2 = "terminal_settings"
    di_new_val = device_settings.get("di_list", {})

    # 接点入力ヘルシー設定状況
    di_state = {}

    # 現在日時を取得
    now = datetime.now()
    now_datetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)

    # 接点入力ヘルシーチェックを無効に設定した場合、現状態を更新する
    # 現状態取得
    device_state = device_state_table.query(
        KeyConditionExpression=Key("device_id").eq(device_id)
    ).get("Items")

    if device_state:
        device_state = device_state[0]
    else:
        return

    for di in di_new_val:
        di_no = di.get("di_no")
        if di is not None:
            di["di_no"] = Decimal(di_no)

        di_healthy_period = di.get("di_healthy_period")
        if di_healthy_period is not None:
            di["di_healthy_period"] = Decimal(di_healthy_period)

        # 接点入力ヘルシーチェックが未設定の場合、1を保持
        if di["di_healthy_period"] == 0 :
            di_state[di_no] = 1
            continue

        current_di_healthy_state = f"di{di_no}_healthy_state"
        if current_di_healthy_state not in device_state or device_state.get(current_di_healthy_state) == 0:
            continue

        di_healthy_period = di.get("di_healthy_period", 0)
        di_healthy_type = di.get("di_healthy_type")
        if di_healthy_type == "hour":
            di_healthy_period_time = di_healthy_period * 60 * 60 * 1000
        else:
            di_healthy_period_time = di_healthy_period * 24 * 60 * 60 * 1000

        di_last_change_datetime = f"di{di_no}_last_change_datetime"
        if di_last_change_datetime in device_state:
            last_recv_datetime = device_state.get(di_last_change_datetime)
            elapsed_time = now_datetime - last_recv_datetime
            if elapsed_time < di_healthy_period_time:
                di_state[di_no] = 1

    di_key = "di_list"
    update_expression = "SET #map.#sub1.#sub2.#di_key = :di_new_val"
    expression_attribute_values = {":di_new_val": di_new_val}
    expression_attribute_name = {
        "#map": map_attribute_name,
        "#sub1": sub_attribute_name1,
        "#sub2": sub_attribute_name2,
        "#di_key": di_key,
    }
    device_table.update_item(
        Key={"device_id": device_id, "identification_id": imei},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )

    # 現状態更新
    option = {
        "Key": {
            "device_id": device_id,
        },
        "UpdateExpression": "set #di1_healthy_state = :di1_healthy_state, \
                            #di2_healthy_state = :di2_healthy_state, \
                            #di3_healthy_state = :di3_healthy_state, \
                            #di4_healthy_state = :di4_healthy_state, \
                            #di5_healthy_state = :di5_healthy_state, \
                            #di6_healthy_state = :di6_healthy_state, \
                            #di7_healthy_state = :di7_healthy_state, \
                            #di8_healthy_state = :di8_healthy_state",
        "ExpressionAttributeNames": {
            "#di1_healthy_state": "di1_healthy_state",
            "#di2_healthy_state": "di2_healthy_state",
            "#di3_healthy_state": "di3_healthy_state",
            "#di4_healthy_state": "di4_healthy_state",
            "#di5_healthy_state": "di5_healthy_state",
            "#di6_healthy_state": "di6_healthy_state",
            "#di7_healthy_state": "di7_healthy_state",
            "#di8_healthy_state": "di8_healthy_state",
        },
        "ExpressionAttributeValues": {
            ":di1_healthy_state": 0 if di_state.get(1) == 1 else device_state.get("di1_healthy_state", 0),
            ":di2_healthy_state": 0 if di_state.get(2) == 1 else device_state.get("di2_healthy_state", 0),
            ":di3_healthy_state": 0 if di_state.get(3) == 1 else device_state.get("di3_healthy_state", 0),
            ":di4_healthy_state": 0 if di_state.get(4) == 1 else device_state.get("di4_healthy_state", 0),
            ":di5_healthy_state": 0 if di_state.get(5) == 1 else device_state.get("di5_healthy_state", 0),
            ":di6_healthy_state": 0 if di_state.get(6) == 1 else device_state.get("di6_healthy_state", 0),
            ":di7_healthy_state": 0 if di_state.get(7) == 1 else device_state.get("di7_healthy_state", 0),
            ":di8_healthy_state": 0 if di_state.get(8) == 1 else device_state.get("di8_healthy_state", 0),
        },
    }

    logger.debug(f"option={option}")
    device_state_table.update_item(**option)

    return