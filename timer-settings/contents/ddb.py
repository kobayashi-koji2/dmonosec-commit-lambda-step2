import uuid
from decimal import Decimal

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

import db

logger = Logger()
dynamodb = boto3.resource("dynamodb")


class SettingLimitError(Exception):
    def __init__(self, message=""):
        self.message = message


# デバイス設定更新
def update_device_settings(device_id, timer_settings, table):
    timer_value = {}

    do_no = timer_settings.get("do_no")

    timer_value["do_timer_id"] = timer_settings.get("do_timer_id", "")
    timer_value["do_timer_name"] = timer_settings.get("do_timer_name", "")
    timer_value["do_onoff_control"] = Decimal(timer_settings.get("do_onoff_control"))
    timer_value["do_time"] = timer_settings.get("do_time", "")
    timer_value["do_weekday"] = timer_settings.get("do_weekday", "")
    logger.info(f"timer_value={timer_value}")

    # 接点出力タイマー一覧を取得
    device_info = db.get_device_info_other_than_unavailable(device_id, table)
    do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
    do_timer_list = []
    for do in do_list:
        if do["do_no"] == do_no:
            if "do_timer_list" in do:
                do_timer_list = do.get("do_timer_list", [])
            else:
                do["do_timer_list"] = do_timer_list
            break
    logger.info(f"do_timer_list={do_timer_list}")

    # 接点出力_タイマーIDがなければ、新規作成
    if not timer_value["do_timer_id"]:
        timer_value["do_timer_id"] = str(uuid.uuid4())
        do_timer_list.append(timer_value)
        logger.info(f"add timer_id={timer_value['do_timer_id']}")
    else:
        update_flag = False
        for do_timer in do_timer_list:
            if do_timer["do_timer_id"] == timer_value["do_timer_id"]:
                update_flag = True
                break
        if update_flag:
            for index, item in enumerate(do_timer_list):
                if item["do_timer_id"] == timer_value["do_timer_id"]:
                    do_timer_list[index] = timer_value
                    logger.info(f"update timer_id={timer_value['do_timer_id']}")
                    break
        else:
            for do in do_list:
                wk_do_timer_list = do.get("do_timer_list", [])
                for do_timer in wk_do_timer_list:
                    if do_timer.get("do_timer_id") == timer_value["do_timer_id"]:
                        wk_do_timer_list.remove(do_timer)
                        logger.info(f"remove timer_id={timer_value['do_timer_id']}")
                        break
            do_timer_list.append(timer_value)
    logger.debug(f"do_list: {do_list}")

    # タイマー設定の上限件数チェック
    timer_count = 0
    for do in do_list:
        timer_count += len(do.get("do_timer_list", []))
    if timer_count > 100:
        raise SettingLimitError("設定可能なスケジュール設定の上限を超えています。")

    update_expression = "SET #map.#sub1.#sub2.#sub3 = :do_list"
    expression_attribute_values = {":do_list": do_list}
    expression_attribute_name = {
        "#map": "device_data",
        "#sub1": "config",
        "#sub2": "terminal_settings",
        "#sub3": "do_list",
    }
    table.update_item(
        Key={"device_id": device_id, "imei": device_info.get("imei")},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name,
    )
    return
