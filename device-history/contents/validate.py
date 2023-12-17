import json
import logging
from datetime import datetime

# layer
import db
import convert

logger = logging.getLogger()


DATE_FORMAT = "%Y/%m/%d %H:%M:%S"


def get_user_device_list(user_id, device_relation_table):
    device_relation_list = db.get_device_relation("u-" + user_id, device_relation_table)
    print(device_relation_list)
    user_device_list = []
    for relation in device_relation_list:
        relation_id = relation["key2"]
        if relation_id.startswith("d-"):
            user_device_list.append(relation_id[2:])
        elif relation_id.startswith("g-"):
            user_device_list.extend(
                [
                    relation_device_id["key2"][2:]
                    for relation_device_id in db.get_device_relation(
                        relation_id, device_relation_table, sk_prefix="d-"
                    )
                ]
            )
    return user_device_list


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
    except Exception as e:
        logger.error(e)
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    # ユーザの存在チェック
    user_res = db.get_user_info_by_user_id(user_id, user_table)
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
            datetime.strptime(params["history_start_datetime"], DATE_FORMAT)
        except ValueError:
            print(ValueError)
            return {"code": "9999", "message": "パラメータが不正です"}

    if params["history_end_datetime"]:
        try:
            datetime.strptime(params["history_end_datetime"], DATE_FORMAT)
        except ValueError:
            print(ValueError)
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

    # 権限チェック（共通）
    for device_id in params["device_list"]:
        if device_id not in contract["contract_data"]["device_list"]:
            return {"code": "9999", "messege": "不正なデバイスIDが指定されています。"}

    # 権限チェック（作業者）
    if user["user_type"] != "admin":
        user_device_list = get_user_device_list(user["user_id"], device_relation_table)
        for device_id in params["device_list"]:
            if device_id not in user_device_list:
                return {"code": "9999", "messege": "不正なデバイスIDが指定されています。"}

    return {
        "code": "0000",
        "user_info": user,
        "decoded_idtoken": decoded_idtoken,
        "request_params": params,
    }
