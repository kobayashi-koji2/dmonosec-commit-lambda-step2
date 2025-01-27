import json

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.validation import SchemaValidationError, validator

logger = Logger()

REQUEST_BODY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,  # 未定義プロパティは許可しない
    "properties": {
        "history_storage_period": {"type": "integer", "minimum": 1, "maximum": 3},
    },
}


def validate_request_body(func):
    def wrapper(event, context, *args, **kwargs):
        try:
            validated_request_body = _validate_request_body(event, context)
        except SchemaValidationError as e:
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
                "body": json.dumps({"message": e.message}, ensure_ascii=False),
            }

        result = func(event, context, *args, validated_request_body, **kwargs)
        return result

    return wrapper


@validator(inbound_schema=REQUEST_BODY_SCHEMA, envelope="powertools_json(body)")
def _validate_request_body(event, context):
    return event


# ユーザ操作権限チェック
def operation_auth_check(user_info, user_type, flag=True):
    """
    ユーザの権限をチェックする関数です。

    Parameters:
        user_info (dict): ユーザ情報を格納した辞書型オブジェクト。
        user_type (str or list): チェックする権限の種類。単一の権限の場合は文字列、複数の権限の場合はリストで指定します。
        flag (bool, optional): チェックの方法を指定するフラグ。Trueの場合は指定権限のみを許可し、Falseの場合は指定権限以外を許可します。デフォルトはTrueです。

    Returns:
        bool: ユーザの権限が指定された条件に一致する場合はTrue、それ以外の場合はFalseを返します。
    """
    user_id = user_info["user_id"]
    op_user_type = user_info["user_type"]
    logger.debug(f"ユーザID: {user_id}, 権限: {op_user_type}")

    # 指定権限のみを許可
    if flag:
        if isinstance(user_type, list):
            result = True if op_user_type in user_type else False
        else:
            result = True if op_user_type == user_type else False
    # 指定権限以外を許可
    else:
        if isinstance(user_type, list):
            result = False if op_user_type in user_type else True
        else:
            result = False if op_user_type == user_type else True

    return result
