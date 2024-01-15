import json

from aws_lambda_powertools import Logger

import db
import ddb

logger = Logger()


# パラメータチェック
def validate(event, user, contract_table, device_relation_table, remote_controls_table):
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"code": "9999", "message": "グループの操作権限がありません。"}

    # 入力値チェック
    path_params = event.get("pathParameters") or {}

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"code": "9999", "message": "アカウント情報が存在しません。"}

    if "device_req_no" not in path_params:
        return {"code": "9999", "message": "パラメータが不正です"}

    remote_control = ddb.get_remote_control_info(
        path_params["device_req_no"], remote_controls_table
    )
    if not remote_control:
        return {"code": "9999", "message": "端末要求番号が存在しません。"}

    # 権限チェック（共通）
    if remote_control.get("device_id") not in contract["contract_data"]["device_list"]:
        return {"code": "9999", "message": "不正なデバイスIDが指定されています。"}

    # 権限チェック（作業者）
    if not operation_auth_check(user):
        user_device_list = db.get_user_relation_device_id_list(
            user["user_id"], device_relation_table
        )
        if remote_control.get("device_id") not in user_device_list:
            return {"code": "9999", "message": "不正なデバイスIDが指定されています。"}

    params = {
        "device_req_no": path_params["device_req_no"],
    }

    return {
        "code": "0000",
        "contract_info": contract,
        "request_params": params,
        "remote_control": remote_control,
    }


# 操作権限チェック
def operation_auth_check(user):
    user_type = user["user_type"]
    logger.debug(f"権限:{user_type}")
    if user_type == "admin" or user_type == "sub_admin":
        return True
    return False
