import json

from aws_lambda_powertools import Logger

import db

logger = Logger()


# パラメータチェック
def validate(event, user, contract_table):
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"code": "9999", "message": "グループの操作権限がありません。"}

    # 入力値チェック
    http_method = event.get("httpMethod")
    body_params = json.loads(event.get("body", "{}"))
    path_params = event.get("pathParameters") or {}

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"code": "9999", "message": "アカウント情報が存在しません。"}

    # グループIDの権限チェック（更新の場合）
    if http_method == "PUT":
        if "group_id" not in path_params:
            return {"code": "9999", "message": "パラメータが不正です"}
        if path_params["group_id"] not in contract["contract_data"]["group_list"]:
            return {"code": "9999", "message": "不正なグループIDが指定されています。"}

    # デバイスIDの権限チェック
    device_list = body_params.get("device_list", [])
    for device_id in device_list:
        if device_id not in contract["contract_data"]["device_list"]:
            return {"code": "9999", "message": "不正なデバイスIDが指定されています。"}

    params = {
        "group_id": path_params.get("group_id"),
        "group_name": body_params.get("group_name", ""),
        "device_list": body_params.get("device_list", []),
    }

    return {
        "code": "0000",
        "contract_info": contract,
        "request_params": params,
    }


# 操作権限チェック
def operation_auth_check(user):
    user_type = user["user_type"]
    logger.debug(f"権限:{user_type}")
    if user_type == "admin" or user_type == "sub_admin":
        return True
    return False
