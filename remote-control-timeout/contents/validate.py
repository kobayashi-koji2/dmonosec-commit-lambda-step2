import json

from aws_lambda_powertools import Logger

import db
import convert
import ddb

logger = Logger()


# TODO 履歴取得のvalidate.pyからコピー 要共通化
def get_user_device_list(user_id, device_relation_table):
    device_relation_list = db.get_device_relation("u-" + user_id, device_relation_table)
    logger.info(device_relation_list)
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
def validate(event, contract_table, user_table, device_relation_table, remote_controls_table):
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
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}

    # ユーザの存在チェック
    user = db.get_user_info_by_user_id(user_id, user_table)
    if not user:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"code": "9999", "message": "グループの操作権限がありません。"}

    # 入力値チェック
    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}

    body_params = json.loads(event.get("body", "{}"))
    if "device_req_no" not in body_params:
        return {"code": "9999", "message": "パラメータが不正です"}

    device_req_no = body_params.get("device_req_no")
    remote_control = ddb.get_remote_control_info(device_req_no, remote_controls_table)
    if not remote_control:
        return {"code": "9999", "message": "端末要求番号が存在しません。"}

    # 権限チェック（共通）
    if remote_control.get("device_id") not in contract["contract_data"]["device_list"]:
        return {"code": "9999", "messege": "不正なデバイスIDが指定されています。"}

    # 権限チェック（作業者）
    if not operation_auth_check(user):
        user_device_list = get_user_device_list(user["user_id"], device_relation_table)
        if remote_control.get("device_id") not in user_device_list:
            return {"code": "9999", "messege": "不正なデバイスIDが指定されています。"}

    params = {
        "device_req_no": device_req_no,
    }

    return {
        "code": "0000",
        "user_info": user,
        "contract_info": contract,
        "decoded_idtoken": decoded_idtoken,
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
