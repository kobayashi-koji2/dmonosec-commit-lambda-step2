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
        return {"message": "パラメータが不正です。"}

    ### 「認証コード」の必須チェック
    if "auth_code" not in body:
        return {"message": "「認証コード」が存在しません。"}
    ### 「auth_code」の型チェック
    if not isinstance(body.get("auth_code"), str):
        return {"message": "「認証コード」のデータ型が不正です。"}

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

            エラーコード：002-0502
        """
        message = textwrap.dedent(message)
        return {"message": message}

    ### 「メールアドレス」の必須チェック
    if "email_address" not in body:
        return {"message": "「メールアドレス」が存在しません。"}
    ### 「メールアドレス」の型チェック
    if not isinstance(body.get("email_address"), str):
        return {"message": "「メールアドレス」のデータ型が不正です。"}

    return {"request_body": body}
