import json

from aws_lambda_powertools import Logger

import db

logger = Logger()


# パラメータチェック
def validate(event, user, contract_table, group_table):
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"message": "グループの操作権限がありません。"}

    # 入力値チェック
    http_method = event.get("httpMethod")
    body_params = json.loads(event.get("body", "{}"))
    path_params = event.get("pathParameters") or {}

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"message": "アカウント情報が存在しません。"}

    # グループ登録数チェック（登録の場合）
    if http_method == "POST":
        group_list = contract.get("contract_data", []).get("group_list", [])
        if len(group_list) >= 300:
            return {"message": "グループ登録可能数の上限に達しています。"}

    # グループIDの権限チェック（更新の場合）
    if http_method == "PUT":
        if "group_id" not in path_params:
            return {"message": "パラメータが不正です"}
        if path_params["group_id"] not in contract["contract_data"]["group_list"]:
            return {"message": "不正なグループIDが指定されています。"}

    # デバイスIDの権限チェック
    device_list = body_params.get("device_list", [])
    for device_id in device_list:
        if device_id not in contract["contract_data"]["device_list"]:
            return {"message": "不正なデバイスIDが指定されています。"}

    # グループ名の重複チェック
    group_name = body_params.get("group_name", "")
    for group_id in contract.get("contract_data", {}).get("group_list", {}):
        if http_method == "PUT" and group_id == path_params.get("group_id"):
            continue
        group = db.get_group_info(group_id, group_table)
        if group.get("group_data", {}).get("config", {}).get("group_name") == group_name:
            return {"message": "グループ名が重複しています。"}

    params = {
        "group_id": path_params.get("group_id"),
        "group_name": group_name,
        "device_list": device_list,
    }

    return {
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
