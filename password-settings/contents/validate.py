import json
import re

from aws_lambda_powertools import Logger

logger = Logger()


# パラメータチェック
def validate(event):
    # リクエストボディのバリデーション
    body = json.loads(event.get("body", {}))
    if not body:
        return {"message": "リクエストボディが不正です。"}
    ### 「password」の必須チェック
    if "password" not in body:
        return {"message": "「password」が存在しません。"}
    ### 「password」の型チェック
    if not isinstance(body.get("password"), str):
        return {"message": "「password」のデータ型が不正です。"}

    ### 「new_password」の必須チェック
    if "new_password" not in body:
        return {"message": "「new_password」が存在しません。"}
    ### 「new_password」の型チェック
    if not isinstance(body.get("new_password"), str):
        return {"message": "「new_password」のデータ型が不正です。"}

    ### 「access_token」の必須チェック
    if "access_token" not in body:
        return {"message": "「access_token」が存在しません。"}
    ### 「access_token」の型チェック
    if not isinstance(body.get("access_token"), str):
        return {"message": "「access_token」のデータ型が不正です。"}

    return {"request_body": body}
