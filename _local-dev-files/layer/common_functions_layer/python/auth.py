import logging

from jose import jwt, JWTError

import db

logger = logging.getLogger()


class AuthError(Exception):
    def __init__(self, code=401, message="Unauthorized"):
        super().__init__(f"{code} {message}")
        self.code = code
        self.message = message


def verify_user(event, user_table):
    try:
        id_token = event["headers"]["Authorization"]
        claims = jwt.get_unverified_claims(id_token)

        user_id = claims["cognito:username"]
        # パスワード有効期限日時
        password_exp = claims["password_exp"]
        # 認証日時
        auth_time = claims["auth_time"]

        password_exp = int(password_exp) if isinstance(password_exp, str) else password_exp
        auth_time = int(auth_time) if isinstance(auth_time, str) else auth_time

    except (JWTError, KeyError, ValueError):
        logger.info("トークンが不正", exc_info=True)
        raise AuthError(401, "認証情報が不正です。")

    user_info = db.get_user_info_by_user_id(user_id, user_table)

    if not user_info:
        raise AuthError(401, "認証情報が不正です。")
    if auth_time > password_exp:
        raise AuthError(401, "パスワードの有効期限が切れています。")

    return user_info
