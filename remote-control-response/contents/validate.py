import json

from aws_lambda_powertools import Logger

import db
import ddb

logger = Logger()


# パラメータチェック
def validate(
    event,
    user,
    contract_table,
    device_relation_table,
    remote_controls_table,
    device_table,
):
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"message": "グループの操作権限がありません。"}

    # 入力値チェック
    path_params = event.get("pathParameters") or {}

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"message": "アカウント情報が存在しません。"}

    if "device_req_no" not in path_params:
        return {"message": "パラメータが不正です"}
    
    # レスポンスヘッダー
    device_req_no = event["pathParameters"]["device_req_no"]
    logger.info(f"device_req_no: {device_req_no}")
    remote_control = db.get_remote_control(device_req_no, remote_controls_table)
    logger.info(f"remote_control: {remote_control}")
    if remote_control is None:
        return {"message": "端末要求番号が存在しません。"}

    # デバイス種別取得
    device_id = remote_control.get("device_id")
    device_info = ddb.get_device_info(device_id, device_table)
    logger.info(f"device_id: {device_id}")
    logger.info(f"device_info: {device_info}")
    if len(device_info) == 0:
        return {"message": "デバイス情報が存在しません。"}
    elif len(device_info) >= 2:
        return {
            "message": "デバイスIDに「契約状態:初期受信待ち」「契約状態:使用可能」の機器が複数紐づいています"
        }
    device_type = device_info[0]["device_type"]

    # デバイス種別チェック
    if device_type == "UnaTag":
        return {"message": "UnaTagに接点入力設定を行うことはできません。"}

    # 権限チェック（共通）
    if remote_control.get("device_id") not in contract["contract_data"]["device_list"]:
        return {"message": "不正なデバイスIDが指定されています。"}

    # 権限チェック（作業者）
    if not operation_auth_check(user):
        user_device_list = db.get_user_relation_device_id_list(
            user["user_id"], device_relation_table
        )
        if device_id not in user_device_list:
            return {"message": "不正なデバイスIDが指定されています。"}

    params = {
        "device_req_no": path_params["device_req_no"],
    }

    return {
        "contract_info": contract,
        "request_params": params,
        "remote_control": remote_control,
    }


# 操作権限チェック
def operation_auth_check(user):
    user_type = user["user_type"]
    logger.debug(f"権限:{user_type}")
    if user_type == "referrer":
        return False
    return True
