import json
import boto3
import traceback

from aws_lambda_powertools import Logger

import db
import convert

logger = Logger()


# パラメータチェック
def validate(event, user_table):
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
        logger.info(traceback.format_exc())
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    # ユーザの存在チェック
    user = db.get_user_info_by_user_id(user_id, user_table)
    if not user:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"code": "9999", "message": "グループの操作権限がありません。"}

    return {
        "code": "0000",
        "user_info": user,
        "decoded_idtoken": decoded_idtoken,
    }


# 操作権限チェック
def operation_auth_check(user):
    user_type = user["user_type"]
    logger.debug(f"権限:{user_type}")
    if user_type == "admin" or user_type == "sub_admin":
        return True
    return False
