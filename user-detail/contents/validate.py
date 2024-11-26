import json
import boto3
import traceback
from aws_lambda_powertools import Logger

import db

logger = Logger()


# パラメータチェック
def validate(event, user, contract_table):
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"message": "ユーザの操作権限がありません。"}

    # 入力値ェック
    path_params = event.get("pathParameters", {})
    params = {"user_id": path_params.get("user_id")}

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"message": "アカウント情報が存在しません。"}

    if not params["user_id"]:
        return {"message": "パラメータが不正です"}
    if params["user_id"] not in contract["contract_data"]["user_list"]:
        return {"message": "削除されたユーザーが選択されました。\n画面の更新を行います。\n\nエラーコード：006-0101"}

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
