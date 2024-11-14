import json
import boto3
import traceback
import db

from aws_lambda_powertools import Logger


logger = Logger()


# パラメータチェック
def validate(event, user, contract_table):
    contract_info = db.get_contract_info(user["contract_id"], contract_table)
    if not contract_info:
        return {"message": "アカウント情報が存在しません。"}
    return {}
