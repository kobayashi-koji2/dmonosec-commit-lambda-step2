import json
import boto3

from aws_lambda_powertools import Logger

# layer
import db
import convert

logger = Logger()


# パラメータチェック
def validate(event):
    # リクエストボディのバリデーション
    body = json.loads(event.get("body", {}))
    if not body:
        return {"message": "パラメータが不正です。"}
    ### 「device_list」の必須チェック
    if "device_list" not in body:
        return {"message": "パラメータが不正です。"}
    ### 「device_list」の型チェック
    device_list = body["device_list"]
    if not isinstance(device_list, list):
        return {"message": "パラメータが不正です。"}

    ### 「device_id」の必須チェック
    if len(body["device_list"]) == 0:
        return {"message": "パラメータが不正です。"}
    for item in device_list:
        ### 「device_id」の型チェック
        if not isinstance(item, str):
            return {"message": "パラメータが不正です。"}

    return {
        "req_body": body,
    }
