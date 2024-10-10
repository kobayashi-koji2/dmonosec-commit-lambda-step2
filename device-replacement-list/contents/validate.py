import json

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.validation import SchemaValidationError, validator

logger = Logger()

PATH_PARAMETERS_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "required": ["identification_id"],
    "properties": {
        "identification_id": {"type": "string"},
    },
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

        result = func(event, context, *args, validated_path_parameters["identification_id"], **kwargs)
        return result

    return wrapper


@validator(inbound_schema=PATH_PARAMETERS_SCHEMA, envelope="pathParameters")
def _validate_path_parameters(event, context):
    return event
