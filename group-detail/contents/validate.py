import json
import boto3
import logging
import traceback

import db
import convert

logger = logging.getLogger()


# パラメータチェック
def validate(event, contract_table, user_table):
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "messege": "リクエストパラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "リクエストパラメータが不正です。"}

    try:
        decoded_idtoken = convert.decode_idtoken(event)
        print("idtoken:", decoded_idtoken)
        user_id = decoded_idtoken["cognito:username"]
    except Exception as e:
        logger.error(e)
        print(traceback.format_exc())
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    # ユーザの存在チェック
    user_res = db.get_user_info_by_user_id(user_id, user_table)
    if "Item" not in user_res:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    user = user_res["Item"]
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"code": "9999", "message": "グループの操作権限がありません。"}

    # 入力値ェック
    path_params = event.get("pathParameters", {})
    params = {"group_id": path_params.get("group_id")}

    contract_res = db.get_contract_info(user["contract_id"], contract_table)
    if "Item" not in contract_res:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}
    contract = contract_res["Item"]

    if not params["group_id"]:
        return {"code": "9999", "message": "パラメータが不正です"}
    if params["group_id"] not in contract["contract_data"]["group_list"]:
        return {"code": "9999", "messege": "不正なグループIDが指定されています。"}

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
