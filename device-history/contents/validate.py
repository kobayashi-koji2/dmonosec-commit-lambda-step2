import json
import boto3
import logging
from datetime import datetime

# layer
import db
import convert

logger = logging.getLogger()


# パラメータチェック
def validate(event, account_table, user_table, contract_table):
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "message": "パラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "パラメータが不正です。"}

    idtoken = event["headers"]["Authorization"]
    try:
        decoded_idtoken = convert.decode_idtoken(idtoken)
        logger.debug("idtoken:", decoded_idtoken)
        user_id = decoded_idtoken["cognito:username"]
    except Exception as e:
        logger.error(e)
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    # ユーザの存在チェック
    user_res = db.get_user_info(user_id, user_table)
    if "Item" not in user_res:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    user = user_res["Item"]

    # 入力値ェック
    body = json.loads(event.get("body", "{}"))
    params = {
        "history_start_datetime": body.get("history_start_datetime"),
        "history_end_datetime": body.get("history_end_datetime"),
        "event_type_list": body.get("event_type_list", []),
        "device_list": body.get("device_list", []),
    }
    print(params)

    if not params["history_start_datetime"] and not params["history_end_datetime"]:
        return {"code": "9999", "message": "パラメータが不正です"}

    if params["history_start_datetime"]:
        try:
            datetime.strptime(params["history_start_datetime"], "%Y%m%d%H%M%S")
        except ValueError:
            return {"code": "9999", "message": "パラメータが不正です"}

    if params["history_end_datetime"]:
        try:
            datetime.strptime(params["history_end_datetime"], "%Y%m%d%H%M%S")
        except ValueError:
            return {"code": "9999", "message": "パラメータが不正です"}

    if (
        params["history_start_datetime"]
        and params["history_end_datetime"]
        and params["history_start_datetime"] > params["history_end_datetime"]
    ):
        return {"code": "9999", "message": "パラメータが不正です"}

    if len(params["device_list"]) == 0:
        return {"code": "9999", "message": "パラメータが不正です"}

    contract_res = db.get_contract_info(user["contract_id"], contract_table)
    if "Item" not in contract_res:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}
    contract = contract_res["Item"]
    print(contract)

    # 権限チェック（共通）
    for device_id in params["device_list"]:
        if device_id not in contract["contract_data"]["device_list"]:
            return {"code": "9999", "messege": "不正なデバイスIDが指定されています。"}

    # 権限チェック（作業者）
    # TODO デバイス関係テーブルを使ってチェック

    return {
        "code": "0000",
        "user_info": user,
        "decoded_idtoken": decoded_idtoken,
        "request_params": params,
    }
