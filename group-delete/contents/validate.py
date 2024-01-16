import json
import os

from aws_lambda_powertools import Logger
import boto3

import db

logger = Logger()


# パラメータチェック
def validate(event, user, contract_table):
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"message": "グループの操作権限がありません。"}

    # 入力値チェック
    path_params = event.get("pathParameters", {})

    contract = db.get_contract_info(user["contract_id"], contract_table)
    if not contract:
        return {"message": "アカウント情報が存在しません。"}

    # グループIDの権限チェック
    if not path_params["group_id"]:
        return {"message": "パラメータが不正です"}
    if path_params["group_id"] not in contract["contract_data"]["group_list"]:
        return {"message": "不正なグループIDが指定されています。"}

    params = {
        "group_id": path_params.get("group_id"),
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
