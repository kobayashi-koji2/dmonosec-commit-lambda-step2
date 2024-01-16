import json
import boto3
import traceback

from aws_lambda_powertools import Logger


logger = Logger()


# パラメータチェック
def validate(event, user):
    operation_auth = operation_auth_check(user)
    if not operation_auth:
        return {"message": "グループの操作権限がありません。"}

    return {}


# 操作権限チェック
def operation_auth_check(user):
    user_type = user["user_type"]
    logger.debug(f"権限:{user_type}")
    if user_type == "admin" or user_type == "sub_admin":
        return True
    return False
