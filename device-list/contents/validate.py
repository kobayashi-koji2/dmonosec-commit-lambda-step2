import json
import boto3

from aws_lambda_powertools import Logger

# layer
import db

logger = Logger()


# パラメータチェック
def validate(event, user_info, tables):
    contract_info = db.get_contract_info(user_info["contract_id"], tables["contract_table"])
    if not contract_info:
        return {"code": "9999", "message": "アカウント情報が存在しません。"}
    return {"code": "0000"}
