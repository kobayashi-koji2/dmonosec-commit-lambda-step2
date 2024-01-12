import json
import os

from aws_lambda_powertools import Logger
import boto3

import db
import convert

logger = Logger()


# パラメータチェック
def validate(event, contract_table, user_table):
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "messege": "リクエストパラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "リクエストパラメータが不正です。"}

    try:
        decoded_idtoken = convert.decode_idtoken(event)
        logger.info(f"idtoken: {decoded_idtoken}")
        user_id = decoded_idtoken["cognito:username"]
    except Exception as e:
        logger.error(e)
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}

    # ユーザの存在チェック
    logger.info(user_id)
    logger.info(user_table)
    user = db.get_user_info_by_user_id(user_id, user_table)
    if not user:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"code": "9999", "message": "グループの操作権限がありません。"}

    # 入力値チェック
    path_params = event.get("pathParameters", {})

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}

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
