import json
import os

import boto3
from aws_lambda_powertools import Logger
from jose import JWTError, jwt

import db
import ssm

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


class AuthError(Exception):
    def __init__(self, code=401, message="Unauthorized"):
        super().__init__(f"{code} {message}")
        self.code = code
        self.message = message


def verify_login_user(func):
    def wrapper(event, *args, **kwargs):
        try:
            login_user = _get_login_user(event)
        except AuthError as e:
            logger.info("ユーザー検証失敗", exc_info=True)
            return {
                "statusCode": e.code,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
                "body": json.dumps({"message": e.message}, ensure_ascii=False),
            }
        result = func(event, *args, login_user, **kwargs)
        return result

    return wrapper


def _get_login_user(event):
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

    user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
    login_user = db.get_user_info_by_user_id(user_id, user_table)

    if not login_user:
        raise AuthError(401, "認証情報が不正です。")
    if auth_time > password_exp:
        raise AuthError(401, "パスワードの有効期限が切れています。")

    return login_user
