import json
import os

import boto3
from aws_lambda_powertools import Logger
from jose import JWTError, jwt
from boto3.dynamodb.conditions import Key

import db
import ssm

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


class AuthError(Exception):
    def __init__(self, code=401, message="Unauthorized"):
        super().__init__(f"{code} {message}")
        self.code = code
        self.message = message


def is_alphanumeric(contract_id):
    return contract_id.isalnum()


def verify_login_user(verify_password_exp=True):
    def _verify_login_user(func):
        def wrapper(event, *args, **kwargs):
            try:
                login_user = _get_login_user(event, verify_password_exp)
            except AuthError as e:
                logger.warning("ユーザー検証失敗", exc_info=True)
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

    return _verify_login_user


def verify_login_user_list(verify_password_exp=True):
    def _verify_login_user_list(func):
        def wrapper(event, *args, **kwargs):
            try:
                login_user_list = _get_login_user_list(event, verify_password_exp)
            except AuthError as e:
                logger.warning("ユーザー検証失敗", exc_info=True)
                return {
                    "statusCode": e.code,
                    "headers": {
                        "Content-Type": "application/json",
                        "Access-Control-Allow-Origin": "*",
                    },
                    "body": json.dumps({"message": e.message}, ensure_ascii=False),
                }
            result = func(event, *args, login_user_list, **kwargs)
            return result

        return wrapper

    return _verify_login_user_list


def _get_login_user(event, verify_password_exp=True):
    try:
        event["headers"] = __convert_to_lower_case(event["headers"])

        id_token = event["headers"]["authorization"]
        claims = jwt.get_unverified_claims(id_token)

        auth_id = claims["custom:auth_id"]

        # 契約コード
        contract_id = event["headers"]["mono-contract-id"]
        if not isinstance(contract_id, str):
            contract_id = str(contract_id)
        if is_alphanumeric(contract_id):
            if len(contract_id) != 8:
                raise AuthError(401, "認証情報が不正です。")
        else:
            raise AuthError(401, "認証情報が不正です。")

        # 認証日時
        auth_time = claims["auth_time"]
        if not isinstance(auth_time, int):
            auth_time = int(auth_time)

        # パスワード有効期限日時
        password_exp = claims["password_exp"]
        if not isinstance(password_exp, int):
            password_exp = int(password_exp)

    except (JWTError, KeyError, ValueError, TypeError) as e:
        raise AuthError(401, "認証情報が不正です。") from e

    account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
    account = db.get_account_info(auth_id, account_table)
    if not account:
        raise AuthError(401, "認証情報が不正です。")

    user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
    res = user_table.query(
        IndexName="account_id_index",
        KeyConditionExpression=Key("account_id").eq(account["account_id"]) & Key("contract_id").eq(contract_id),
    ).get("Items", [])
    user_list = [
        item
        for item in res
        if item.get("user_data", {}).get("config", {}).get("del_datetime") is None
    ]
    login_user = user_list[0] if user_list else None

    if not login_user:
        raise AuthError(401, "認証情報が不正です。")
    if verify_password_exp and auth_time > password_exp:
        raise AuthError(401, "パスワードの有効期限が切れています。")

    return login_user


def _get_login_user_list(event, verify_password_exp=True):
    try:
        event["headers"] = __convert_to_lower_case(event["headers"])

        id_token = event["headers"]["authorization"]
        claims = jwt.get_unverified_claims(id_token)

        auth_id = claims["custom:auth_id"]

        # 認証日時
        auth_time = claims["auth_time"]
        if not isinstance(auth_time, int):
            auth_time = int(auth_time)

        # パスワード有効期限日時
        password_exp = claims["password_exp"]
        if not isinstance(password_exp, int):
            password_exp = int(password_exp)

    except (JWTError, KeyError, ValueError, TypeError) as e:
        raise AuthError(401, "認証情報が不正です。") from e

    account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
    account = db.get_account_info(auth_id, account_table)
    if not account:
        raise AuthError(401, "認証情報が不正です。")

    user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
    res = user_table.query(
        IndexName="account_id_index",
        KeyConditionExpression=Key("account_id").eq(account["account_id"]),
    ).get("Items", [])
    user_list = [
        item
        for item in res
        if item.get("user_data", {}).get("config", {}).get("del_datetime") is None
    ]
    if not user_list:
        raise AuthError(401, "認証情報が不正です。")
    if verify_password_exp and auth_time > password_exp:
        raise AuthError(401, "パスワードの有効期限が切れています。")

    return user_list

 # 項目名を小文字に変換
def __convert_to_lower_case(header):
    return dict(map(lambda kv: (kv[0].lower(), kv[1]), header.items()))