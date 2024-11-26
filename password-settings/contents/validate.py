import json
import re
import textwrap

from aws_lambda_powertools import Logger

logger = Logger()
punctuation = r"\*\+\.\?\)\]\}\{\(\[\^\$\-\|\/\"!@#%&,>\\ <':;_~`="
password_policy = (
    r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*["
    + punctuation
    + r"])[A-Za-z\d{"
    + punctuation
    + r"}]{8,}$"
)


# パラメータチェック
def validate(event):
    # リクエストボディのバリデーション
    body = json.loads(event.get("body", {}))
    if not body:
        return {"message": "リクエストボディが不正です。"}
    ### 「現在のパスワード」の必須チェック
    if "password" not in body:
        return {"message": "「現在のパスワード」が存在しません。"}
    ### 「現在のパスワード」の型チェック
    if not isinstance(body.get("password"), str):
        return {"message": "「現在のパスワード」のデータ型が不正です。"}

    ### 「新しいパスワード」の必須チェック
    if "new_password" not in body:
        return {"message": "「新しいパスワード」が存在しません。"}
    ### 「新しいパスワード」の型チェック
    if not isinstance(body.get("new_password"), str):
        return {"message": "「新しいパスワード」のデータ型が不正です。"}
    ### 「新しいパスワード」の形式チェック
    if not re.search(password_policy, body.get("new_password").strip()):
        message = """
            新しいパスワードがポリシーを満たしていません

            ＜パスワードポリシー＞
            - 8文字以上
            - 英大文字と英小文字を含む
            - 数字を含む
            - 記号を含む

            エラーコード：007-0102
        """
        message = textwrap.dedent(message)
        return {"message": message}

    ### 「access_token」の必須チェック
    if "access_token" not in body:
        return {"message": "「access_token」が存在しません。"}
    ### 「access_token」の型チェック
    if not isinstance(body.get("access_token"), str):
        return {"message": "「access_token」のデータ型が不正です。"}

    return {"request_body": body}
