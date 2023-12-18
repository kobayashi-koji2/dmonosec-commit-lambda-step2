import json
import logging
import os

import boto3

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
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}

    # ユーザの存在チェック
    print(user_id)
    print(user_table)
    user_res = db.get_user_info_by_user_id(user_id, user_table)
    if not "Item" in user_res:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    user = user_res["Item"]
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"code": "9999", "message": "グループの操作権限がありません。"}

    # 入力値チェック
    path_params = event.get("pathParameters", {})

    contract_res = db.get_contract_info(user["contract_id"], contract_table)
    if "Item" not in contract_res:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}
    contract = contract_res["Item"]

    # グループIDの権限チェック
    if not path_params["group_id"]:
        return {"code": "9999", "message": "パラメータが不正です"}
    if path_params["group_id"] not in contract["contract_data"]["group_list"]:
        return {"code": "9999", "messege": "不正なグループIDが指定されています。"}

    params = {
        "group_id": path_params.get("group_id"),
    }

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
