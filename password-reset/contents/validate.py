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

    ### 「auth_code」の必須チェック
    if "auth_code" not in body:
        return {"message": "「auth_code」が存在しません。"}
    ### 「auth_code」の型チェック
    if not isinstance(body.get("auth_code"), str):
        return {"message": "「auth_code」のデータ型が不正です。"}

    ### 「new_password」の必須チェック
    if "new_password" not in body:
        return {"message": "「new_password」が存在しません。"}
    ### 「new_password」の型チェック
    if not isinstance(body.get("new_password"), str):
        return {"message": "「new_password」のデータ型が不正です。"}

    ### 「email_address」の必須チェック
    if "email_address" not in body:
        return {"message": "「email_address」が存在しません。"}
    ### 「email_address」の型チェック
    if not isinstance(body.get("email_address"), str):
        return {"message": "「email_address」のデータ型が不正です。"}

    return {"request_body": body}
