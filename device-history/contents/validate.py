import json
import traceback
import re
from datetime import datetime

from aws_lambda_powertools import Logger

# layer
import db

logger = Logger()


def is_valid_uuid(uuid_string):
    uuid_regex = re.compile(
        r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\Z', re.I)
    return bool(uuid_regex.match(uuid_string))


def contains_device_id_and_last_hist_id(device_list):
    return all(
        "device_id" in device and "last_hist_id" in device and
        (device["device_id"] == "" or is_valid_uuid(device["device_id"])) and
        (device["last_hist_id"] == "" or is_valid_uuid(device["last_hist_id"]))
        for device in device_list
    )


def is_numeric(value):
    if isinstance(value, int):
        return True
    if isinstance(value, str) and value.isdigit():
        return True
    return False


# パラメータチェック
def validate(event, user, account_table, user_table, contract_table, device_relation_table):
    # 入力値チェック
    query_params = event.get("queryStringParameters", {})
    if not ("history_start_datetime" in query_params and
            "history_end_datetime" in query_params and
            "sort" in query_params and
            "limit" in query_params):
        return {"message": "パラメータが不正です"}

    if not (query_params.get("history_start_datetime").isdigit() and
            query_params.get("history_end_datetime").isdigit() and
            is_numeric(query_params.get("sort")) and
            is_numeric(query_params.get("limit"))):
        return {"message": "パラメータが不正です"}

    multi_query_params = event.get("multiValueQueryStringParameters", {})
    if not ("event_type_list[]" in multi_query_params and
            "device_list[]" in multi_query_params):
        return {"message": "パラメータが不正です"}

    try:
        param_device_list = [
            json.loads(device_param)
            for device_param in multi_query_params.get("device_list[]", [])
        ]
    except json.JSONDecodeError:
        return {"message": "パラメータが不正です"}

    params = {
        "history_start_datetime": query_params.get("history_start_datetime"),
        "history_end_datetime": query_params.get("history_end_datetime"),
        "event_type_list": multi_query_params.get("event_type_list[]", []),
        "device_list": param_device_list,
        "sort": int(query_params.get("sort", "1")),
        "limit": int(query_params.get("limit", "50")),
    }
    logger.info(params)

    if not params["history_start_datetime"] and not params["history_end_datetime"]:
        return {"message": "パラメータが不正です"}

    if params["history_start_datetime"]:
        if len(params["history_start_datetime"]) != 10:
            return {"message": "パラメータが不正です"}
        try:
            datetime.fromtimestamp(int(params["history_start_datetime"]))
        except ValueError:
            logger.info(ValueError)
            return {"message": "パラメータが不正です"}
        except OverflowError:
            logger.info(OverflowError)
            return {"message": "パラメータが不正です"}

    if params["history_end_datetime"]:
        if len(params["history_end_datetime"]) != 10:
            return {"message": "パラメータが不正です"}
        try:
            datetime.fromtimestamp(int(params["history_end_datetime"]))
        except ValueError:
            logger.info(ValueError)
            return {"message": "パラメータが不正です"}
        except OverflowError:
            logger.info(OverflowError)
            return {"message": "パラメータが不正です"}

    if (
        params["history_start_datetime"]
        and params["history_end_datetime"]
        and int(params["history_start_datetime"]) > int(params["history_end_datetime"])
    ):
        return {"message": "パラメータが不正です"}

    if not contains_device_id_and_last_hist_id(params["device_list"]):
        return {"message": "パラメータが不正です"}

    if len(params["device_list"]) == 0:
        return {"message": "パラメータが不正です"}

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"message": "アカウント情報が存在しません。"}

    # 権限チェック（共通）
    for device in params["device_list"]:
        if device["device_id"] not in contract["contract_data"]["device_list"]:
            return {"message": "不正なデバイスIDが指定されています。"}

    # 権限チェック（作業者）
    if user["user_type"] != "admin" and user["user_type"] != "sub_admin":
        user_device_list = db.get_user_relation_device_id_list(
            user["user_id"], device_relation_table
        )
        logger.debug(user_device_list)
        for device in params["device_list"]:
            if device["device_id"] not in user_device_list:
                return {"message": "不正なデバイスIDが指定されています。"}

    return {
        "user_info": user,
        "request_params": params,
    }
