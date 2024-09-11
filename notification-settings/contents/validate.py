import json
import db

from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.validation import SchemaValidationError, envelopes, validator

logger = Logger()

BODY_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema",
    "type": "object",
    "required": ["notification_list", "notification_target_list"],
    "properties": {
        "notification_list": {
            "type": "array",
            "minItems": 0,
            "items": {
                "type": "object",
                "required": ["event_trigger"],
                "properties": {
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
                        "type": "integer",
                    },
                    "custom_event_id": {
                        "type": "string",
                    },
                },
            },
        },
        "notification_target_list": {
            "type": "array",
            "minItems": 0,
            "items": {
                "type": "string",
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


# パラメータチェック
def validate(event, body, device_table):
    device_id = event.get("pathParameters", {}).get("device_id")

    # 契約状態チェック
    device = db.get_device_info_other_than_unavailable(device_id, device_table)
    if not device:
        return {"message": "デバイス情報が存在しません。"}

    # 入力値チェック    
    notification_list = body["notification_list"]
    for notification in notification_list:
        event_trigger = notification.get("event_trigger")
        terminal_no = notification.get("terminal_no")
        event_type = notification.get("event_type")
        change_detail = notification.get("change_detail")
        custom_event_id = notification.get("custom_event_id")

        if event_trigger == "di_change":
            if event_type:
                return {"message": "パラメータが不正です"}

            if device["device_type"] == "PJ1":
                if terminal_no != 1:
                    return {"message": "パラメータが不正です"}
            elif device["device_type"] == "PJ2":
                if 1 > terminal_no  or terminal_no > 8:
                    return {"message": "パラメータが不正です"}
            else:
                return {"message": "パラメータが不正です"}

            if change_detail not in [0, 1, 2]:
                return {"message": "パラメータが不正です"}
            
            if custom_event_id:
                return {"message": "パラメータが不正です"}

        elif event_trigger == "do_change":
            if event_type:
                return {"message": "パラメータが不正です"}

            if device["device_type"] == "PJ2":
                if terminal_no not in [1, 2]:
                    return {"message": "パラメータが不正です"}
            else:
                return {"message": "パラメータが不正です"}

            if change_detail != 1:
                return {"message": "パラメータが不正です"}
            
            if custom_event_id:
                return {"message": "パラメータが不正です"}

        elif event_trigger == "device_change":
            if event_type not in ["device_unhealthy", "battery_near", "device_abnormality",\
                                  "parameter_abnormality", "fw_update_abnormality", "power_on"]:
                return {"message": "パラメータが不正です"}

            if terminal_no != 0:
                return {"message": "パラメータが不正です"}

            if change_detail != 1:
                return {"message": "パラメータが不正です"}
            
            if not custom_event_id:
                return {"message": "パラメータが不正です"}

        elif event_trigger == "custom_event":
            if event_type:
                return {"message": "パラメータが不正です"}

            if terminal_no:
                return {"message": "パラメータが不正です"}

            if change_detail:
                return {"message": "パラメータが不正です"}
            
            if not custom_event_id["custom_event_id"]:
                return {"message": "パラメータが不正です"}
            
        else:   
            return {"message": "パラメータが不正です"}
    return {}
