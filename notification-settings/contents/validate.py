import json

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.validation import SchemaValidationError, envelopes, validator

logger = Logger()

BODY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "required": ["notification_list"],
    "properties": {
        "notification_list": {
            "type": "array",
            "minItems": 0,
            "items": {
                "type": "object",
                "required": ["device_id", "event_trigger"],
                "properties": {
                    "device_id": {
                        "type": "string",
                    },
                    "event_trigger": {
                        "type": "string",
                    },
                    "terminal_no": {
                        "type": "integer",
                    },
                    "event_type": {
                        "type": "string",
                    },
                    "change_detail": {
                        "type": "string",
                    },
                    "notification_target_list": {
                        "type": "array",
                        "minItems": 0,
                        "items": {
                            "type": "string",
                        },
                    },
                },
            },
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
