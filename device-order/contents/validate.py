import json
import boto3

from aws_lambda_powertools import Logger

# layer
import db
import convert

logger = Logger()


# パラメータチェック
def validate(event, user_table):
    # ユーザの存在チェック
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "message": "パラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "パラメータが不正です。"}

    decoded_idtoken = convert.decode_idtoken(event)
    if decoded_idtoken == False:
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    user_id = decoded_idtoken["cognito:username"]
    user_info = db.get_user_info_by_user_id(user_id, user_table)
    if not "Item" in user_info:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}

    # リクエストボディのバリデーション
    body = json.loads(event.get("body", {}))
    if not body:
        return {"code": "9999", "message": "パラメータが不正です。"}
    ### 「device_list」の必須チェック
    if "device_list" not in body:
        return {"code": "9999", "messege": "パラメータが不正です。"}
    ### 「device_list」の型チェック
    device_list = body["device_list"]
    if not isinstance(device_list, list):
        return {"code": "9999", "messege": "パラメータが不正です。"}

    ### 「device_id」の必須チェック
    if len(body["device_list"]) == 0:
        return {"code": "9999", "messege": "パラメータが不正です。"}
    for item in device_list:
        ### 「device_id」の型チェック
        if not isinstance(item, str):
            return {"code": "9999", "messege": "パラメータが不正です。"}

    return {
        "code": "0000",
        "user_info": user_info,
        "decoded_idtoken": decoded_idtoken,
        "req_body": body,
    }
