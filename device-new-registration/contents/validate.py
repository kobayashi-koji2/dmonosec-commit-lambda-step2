import json

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.validation import SchemaValidationError, validator

logger = Logger()

PATH_PARAMETERS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "required": ["user_id"],
    "properties": {"user_id": {"type": "string"}},
}

REQUEST_BODY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "required": ["device_imei"],
    "properties": {"device_imei": {"type": "string"}},
}


def validate_parameter(func):
    def wrapper(event, context, *args, **kwargs):
        try:
            validated_path_parameters = _validate_path_parameters(event, context)
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

        result = func(event, context, *args, validated_path_parameters["user_id"], **kwargs)
        return result

    return wrapper


@validator(inbound_schema=PATH_PARAMETERS_SCHEMA, envelope="pathParameters")
def _validate_path_parameters(event, context):
    return event


def validate_request_body(func):
    def wrapper(event, context, *args, **kwargs):
        try:
            validated_request_body = _validate_request_body(event, context)
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

        result = func(event, context, *args, validated_request_body, **kwargs)
        return result

    return wrapper


@validator(inbound_schema=REQUEST_BODY_SCHEMA, envelope="powertools_json(body)")
def _validate_request_body(event, context):
    return event
