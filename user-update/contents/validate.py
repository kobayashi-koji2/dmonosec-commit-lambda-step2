import re
import json
import traceback

from aws_lambda_powertools import Logger
import boto3

import db
import convert

logger = Logger()


def isValidEmail(str):
    pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    return re.match(pattern, str) is not None


# パラメータチェック
def validate(event, contract_table, user_table):
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "messege": "リクエストパラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "リクエストパラメータが不正です。"}

    try:
        decoded_idtoken = convert.decode_idtoken(event)
        logger.info("idtoken:", decoded_idtoken)
        user_id = decoded_idtoken["cognito:username"]
    except Exception as e:
        logger.error(e)
        logger.info(traceback.format_exc())
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    # ユーザの存在チェック
    user = db.get_user_info_by_user_id(user_id, user_table)
    if not user:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"code": "9999", "message": "ユーザの操作権限がありません。"}

    # 入力値ェック
    http_method = event.get("httpMethod")
    body_params = json.loads(event.get("body", "{}"))
    path_params = event.get("pathParameters", {})

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}

    if http_method == "POST":
        if "email_address" not in body_params:
            return {"code": "9999", "message": "パラメータが不正です"}
        elif not isValidEmail(body_params["email_address"]):
            return {"code": "9999", "message": "パラメータが不正です"}

    if "user_type" not in body_params:
        return {"code": "9999", "message": "パラメータが不正です"}

    # 更新対象ユーザのチェック
    if http_method == "PUT":
        if "user_id" not in path_params:
            return {"code": "9999", "message": "パラメータが不正です"}
        update_user_res = db.get_user_info_by_user_id(path_params["user_id"], user_table)
        if not update_user_res:
            return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
        if path_params["user_id"] not in contract["contract_data"]["user_list"]:
            return {"code": "9999", "messege": "不正なユーザIDが指定されています。"}

    # グループの権限チェック
    group_list = body_params.get("management_group_list", [])
    for group_id in group_list:
        if group_id not in contract["contract_data"]["group_list"]:
            return {"code": "9999", "messege": "不正なグループIDが指定されています。"}

    # デバイスの権限チェック
    device_list = body_params.get("management_device_list", [])
    for device_id in device_list:
        if device_id not in contract["contract_data"]["device_list"]:
            return {"code": "9999", "messege": "不正なデバイスIDが指定されています。"}

    params = {
        "update_user_id": path_params.get("user_id"),
        "user_name": body_params.get("user_name", ""),
        "user_type": body_params.get("user_type"),
        "management_group_list": body_params.get("management_group_list"),
        "management_device_list": body_params.get("management_device_list"),
    }
    if http_method == "POST":
        params["email_address"] = body_params.get("email_address").lower()

    return {
        "code": "0000",
        "user_info": user,
        "contract_info": contract,
        "decoded_idtoken": decoded_idtoken,
        "request_params": params,
    }


# 操作権限チェック
def operation_auth_check(user):
    user_type = user["user_type"]
    logger.debug(f"権限:{user_type}")
    if user_type == "admin" or user_type == "sub_admin":
        return True
    return False
