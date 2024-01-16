import json
import traceback
from datetime import datetime

from aws_lambda_powertools import Logger

# layer
import db

logger = Logger()


# パラメータチェック
def validate(event, user, account_table, user_table, contract_table, device_relation_table):
    # 入力値ェック
    query_params = event.get("queryStringParameters", {})
    multi_query_params = event.get("multiValueQueryStringParameters", {})

    params = {
        "history_start_datetime": query_params.get("history_start_datetime"),
        "history_end_datetime": query_params.get("history_end_datetime"),
        "event_type_list": multi_query_params.get("event_type_list[]", []),
        "device_list": multi_query_params.get("device_list[]", []),
    }
    logger.info(params)

    if not params["history_start_datetime"] and not params["history_end_datetime"]:
        return {"message": "パラメータが不正です"}

    if params["history_start_datetime"]:
        try:
            datetime.fromtimestamp(int(params["history_start_datetime"]))
        except ValueError:
            logger.info(ValueError)
            return {"message": "パラメータが不正です"}

    if params["history_end_datetime"]:
        try:
            datetime.fromtimestamp(int(params["history_end_datetime"]))
        except ValueError:
            logger.info(ValueError)
            return {"message": "パラメータが不正です"}

    if (
        params["history_start_datetime"]
        and params["history_end_datetime"]
        and int(params["history_start_datetime"]) > int(params["history_end_datetime"])
    ):
        return {"message": "パラメータが不正です"}

    if len(params["device_list"]) == 0:
        return {"message": "パラメータが不正です"}

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"message": "アカウント情報が存在しません。"}

    # 権限チェック（共通）
    for device_id in params["device_list"]:
        if device_id not in contract["contract_data"]["device_list"]:
            return {"message": "不正なデバイスIDが指定されています。"}

    # 権限チェック（作業者）
    if user["user_type"] != "admin" and user["user_type"] != "sub_admin":
        user_device_list = db.get_user_relation_device_id_list(
            user["user_id"], device_relation_table
        )
        logger.debug(user_device_list)
        for device_id in params["device_list"]:
            if device_id not in user_device_list:
                return {"message": "不正なデバイスIDが指定されています。"}

    return {
        "user_info": user,
        "request_params": params,
    }
