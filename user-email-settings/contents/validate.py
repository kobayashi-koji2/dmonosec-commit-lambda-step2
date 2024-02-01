import json

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.validation import SchemaValidationError, envelopes, validator

logger = Logger()

BODY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "required": ["access_token", "auth_code", "new_email"],
    "properties": {
        "access_token": {
            "type": "string",
            "pattern": "^[A-Za-z0-9-_=.]+$",
        },
        "auth_code": {
            "type": "string",
            "pattern": r"^\S+$",
        },
        "new_email": {
            "type": "string",
            "pattern": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
        },
    },
}


def validate_parameter(func):
    def wrapper(event, context, *args, **kwargs):
        try:
            validated_body = _validate_body(event, context)
        except SchemaValidationError as e:
            logger.info("バリデーションエラー", exc_info=True)
            return {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*",
                },
                "body": json.dumps({"message": e.message}, ensure_ascii=False),
            }

        result = func(event, context, *args, validated_body, **kwargs)
        return result

    return wrapper


@validator(inbound_schema=BODY_SCHEMA, envelope=envelopes.API_GATEWAY_REST)
def _validate_body(event, context):
    return event
