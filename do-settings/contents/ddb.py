import uuid
from decimal import Decimal
from itertools import chain

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

import convert
import db
import ssm

logger = Logger()
dynamodb = boto3.resource("dynamodb")


# デバイス情報取得(契約状態:使用不可以外)
def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    )
    return response


# 連動制御情報取得
def get_automation_info_list(control_device_id, table):
    response = table.query(
        IndexName="control_device_id_index",  # TODO 連動制御管理テーブル追加時に変更の可能性あり
        KeyConditionExpression=Key("control_device_id").eq(control_device_id)
    )
    return response


# デバイス設定更新
def update_device_settings(device_id, imei, device_settings, table):
    map_attribute_name = "device_data"
    sub_attribute_name1 = "config"
    sub_attribute_name2 = "terminal_settings"
    device_name = device_settings.get("device_name")
    device_healthy_period = device_settings.get("device_healthy_period")
    if device_healthy_period is not None:
        device_healthy_period = Decimal(device_healthy_period)
    di_new_val = device_settings.get("di_list", {})
    do_new_val = device_settings.get("do_list", {})
    ai_new_val = device_settings.get("ai_list", {})
    for di in di_new_val:
        di_no = di.get("di_no")
        if di is not None:
            di["di_no"] = Decimal(di_no)

        di_healthy_period = di.get("di_healthy_period")
        if di_healthy_period is not None:
            di["di_healthy_period"] = Decimal(di_healthy_period)

    for do in do_new_val:
        do_no = do.get("do_no")
        if do is not None:
            do["do_no"] = Decimal(do_no)

        do_specified_time = do.get("do_specified_time")
        if do_specified_time is not None:
            do["do_specified_time"] = Decimal(do_specified_time)

        do_di_return = do.get("do_di_return")
        if do_di_return is not None:
            do["do_di_return"] = Decimal(do_di_return)

        do_timer_list = do.get("do_timer_list", [])
        for do_timer in do_timer_list:
            do_onoff_control = do_timer.get("do_onoff_control")
            if do_onoff_control is not None:
                do_timer["do_onoff_control"] = Decimal(do_onoff_control)

    for ai in ai_new_val:
        ai_no = ai.get("ai_no")
        if ai_no is not None:
            ai["ai_no"] = Decimal(ai_no)

    di_key, do_key, ai_key, device_name_key, device_healthy_period_key = "di_list", "do_list", "ai_list", "device_name", "device_healthy_period"
    update_expression = "SET #map.#sub1.#device_name_key = :device_name,\
                        #map.#sub1.#device_healthy_period_key = :device_healthy_period,\
                        #map.#sub1.#sub2.#di_key = :di_new_val,\
                        #map.#sub1.#sub2.#do_key = :do_new_val,\
                        #map.#sub1.#sub2.#ai_key = :ai_new_val"
    expression_attribute_values = {
        ":di_new_val": di_new_val,
        ":do_new_val": do_new_val,
        ":ai_new_val": ai_new_val,
        ":device_name": device_name,
        ":device_healthy_period": device_healthy_period,
    }
    expression_attribute_name = {
        "#map": map_attribute_name,
        "#sub1": sub_attribute_name1,
        "#sub2": sub_attribute_name2,
        "#di_key": di_key,
        "#do_key": do_key,
        "#ai_key": ai_key,
        "#device_name_key": device_name_key,
        "#device_healthy_period_key": device_healthy_period_key,
    }
    table.update_item(
        Key={"device_id": device_id, "imei": imei},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )
    return


# 連動制御情報更新
def sync_automation_info_list(control_device_id, convert_param, table):
    # DynamoDBから現在の項目を取得
    current_automation_info_list = table.query(
        IndexName="control_device_id_index",  # TODO 連動制御管理テーブル追加時に変更の可能性あり
        KeyConditionExpression=Key("control_device_id").eq(control_device_id)
    ).get("Items", [])
    automation_id_to_delete = {item["automation_id"] for item in current_automation_info_list}

    # リクエストボディで受け取った項目を整形
    automation_info_list = chain.from_iterable(
        [
            do_list_item["do_automation_list"] for do_list_item in convert_param.get("do_list", [])
            if "do_automation_list" in do_list_item
        ]
    )
    transact_items = []
    for automation_info in automation_info_list:
        trigger_terminal_no = automation_info.get("trigger_terminal_no")
        if trigger_terminal_no is not None:
            automation_info["trigger_terminal_no"] = Decimal(trigger_terminal_no)

        trigger_event_detail_state = automation_info.get("trigger_event_detail_state")
        if trigger_event_detail_state is not None:
            automation_info["trigger_event_detail_state"] = Decimal(trigger_event_detail_state)

        trigger_event_detail_flag = automation_info.get("trigger_event_detail_flag")
        if trigger_event_detail_flag is not None:
            automation_info["trigger_event_detail_flag"] = Decimal(trigger_event_detail_flag)

        control_do_no = automation_info.get("control_do_no")
        if control_do_no is not None:
            automation_info["control_do_no"] = Decimal(control_do_no)

        control_di_state = automation_info.get("control_di_state")
        if control_di_state is not None:
            automation_info["control_di_state"] = Decimal(control_di_state)

        if automation_info.get("automation_id") == "":
            # アイテムを作成
            transact_items.append(_create_new_automation_info_format(automation_info, control_device_id))
        else:
            # アイテムを更新
            transact_items.append(_update_existing_automation_info_format(automation_info, control_device_id))
            automation_id_to_delete.discard(automation_info["automation_id"])

    for automation_id in automation_id_to_delete:
        # アイテムを削除
        transact_items.append(_delete_automation_info_format(automation_id))

    transact_result = db.execute_transact_write_item(transact_items)
    return transact_result


def _create_new_automation_info_format(automation_info, control_device_id):
    item = convert.dict_dynamo_format({
        "automation_id": str(uuid.uuid4()),
        "trigger_device_id": automation_info.get("trigger_device_id"),
        "trigger_event_type": automation_info.get("trigger_event_type"),
        "trigger_terminal_no": automation_info.get("trigger_terminal_no"),
        "trigger_event_detail_state": automation_info.get("trigger_event_detail_state"),
        "trigger_event_detail_flag": automation_info.get("trigger_event_detail_flag"),
        "control_device_id": control_device_id,
        "control_do_no": automation_info.get("control_do_no"),
        "control_di_state": automation_info.get("control_di_state"),
    })
    return {
        "Put": {
            "TableName": ssm.table_names["AUTOMATION_TABLE"],  # TODO 連動制御管理テーブル追加時に変更の可能性あり
            "Item": item,
            "ConditionExpression": "attribute_not_exists(automation_id)"
        }
    }


def _update_existing_automation_info_format(automation_info, control_device_id):
    return {
        "Update": {
            "TableName": ssm.table_names["AUTOMATION_TABLE"],  # TODO 連動制御管理テーブル追加時に変更の可能性あり
            "Key": {
                "automation_id": convert.to_dynamo_format(automation_info.get("automation_id"))
            },
            "UpdateExpression": "set trigger_device_id = :trigger_device_id, \
                                trigger_event_type = :trigger_event_type, \
                                trigger_terminal_no = :trigger_terminal_no, \
                                trigger_event_detail_state = :trigger_event_detail_state, \
                                trigger_event_detail_flag = :trigger_event_detail_flag, \
                                control_device_id = :control_device_id, \
                                control_do_no = :control_do_no, \
                                control_di_state = :control_di_state",
            "ExpressionAttributeValues": {
                ":trigger_device_id": convert.to_dynamo_format(automation_info.get("trigger_device_id")),
                ":trigger_event_type": convert.to_dynamo_format(automation_info.get("trigger_event_type")),
                ":trigger_terminal_no": convert.to_dynamo_format(automation_info.get("trigger_terminal_no")),
                ":trigger_event_detail_state": convert.to_dynamo_format(
                    automation_info.get("trigger_event_detail_state")
                ),
                ":trigger_event_detail_flag": convert.to_dynamo_format(
                    automation_info.get("trigger_event_detail_flag")
                ),
                ":control_device_id": convert.to_dynamo_format(control_device_id),
                ":control_do_no": convert.to_dynamo_format(automation_info.get("control_do_no")),
                ":control_di_state": convert.to_dynamo_format(automation_info.get("control_di_state")),
            },
            "ConditionExpression": "attribute_exists(automation_id)"
        }
    }


def _delete_automation_info_format(automation_id):
    return {
        "Delete": {
            "TableName": ssm.table_names["AUTOMATION_TABLE"],  # TODO 連動制御管理テーブル追加時に変更の可能性あり
            "Key": {
                "automation_id": convert.to_dynamo_format(automation_id)
            },
        }
    }
