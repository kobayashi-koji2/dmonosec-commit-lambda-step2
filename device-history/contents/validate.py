import json
import traceback
from datetime import datetime

from aws_lambda_powertools import Logger

# layer
import db
import convert

logger = Logger()


# パラメータチェック
def validate(event, account_table, user_table, contract_table, device_relation_table):
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "message": "パラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "パラメータが不正です。"}

    try:
        decoded_idtoken = convert.decode_idtoken(event)
        logger.debug("idtoken:", decoded_idtoken)
        user_id = decoded_idtoken["cognito:username"]
    except Exception:
        logger.info(traceback.format_exc())
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    # ユーザの存在チェック
    user = db.get_user_info_by_user_id(user_id, user_table)
    if not user:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}

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
        return {"code": "9999", "message": "パラメータが不正です"}

    if params["history_start_datetime"]:
        try:
            datetime.fromtimestamp(int(params["history_start_datetime"]))
        except ValueError:
            logger.info(ValueError)
            return {"code": "9999", "message": "パラメータが不正です"}

    if params["history_end_datetime"]:
        try:
            datetime.fromtimestamp(int(params["history_end_datetime"]))
        except ValueError:
            logger.info(ValueError)
            return {"code": "9999", "message": "パラメータが不正です"}

    if (
        params["history_start_datetime"]
        and params["history_end_datetime"]
        and int(params["history_start_datetime"]) > int(params["history_end_datetime"])
    ):
        return {"code": "9999", "message": "パラメータが不正です"}

    if len(params["device_list"]) == 0:
        return {"code": "9999", "message": "パラメータが不正です"}

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}

    # 権限チェック（共通）
    for device_id in params["device_list"]:
        if device_id not in contract["contract_data"]["device_list"]:
            return {"code": "9999", "messege": "不正なデバイスIDが指定されています。"}

    # 権限チェック（作業者）
    if user["user_type"] != "admin" and user["user_type"] != "sub_admin":
        user_device_list = db.get_user_relation_device_id_list(
            user["user_id"], device_relation_table
        )
        logger.debug(user_device_list)
        for device_id in params["device_list"]:
            if device_id not in user_device_list:
                return {"code": "9999", "messege": "不正なデバイスIDが指定されています。"}

    return {
        "code": "0000",
        "user_info": user,
        "decoded_idtoken": decoded_idtoken,
        "request_params": params,
    }
