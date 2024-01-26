import json
import os

import boto3
from aws_lambda_powertools import Logger

import auth
import db
import ssm
import convert

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user
def lambda_handler(event, context, user):
    res_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }
    logger.info({"user": user})

    # DynamoDB操作オブジェクト生成
    try:
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
    except KeyError as e:
        body = {"message": e}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }

    try:
        if user.get("user_type") != "admin" and user.get("user_type") != "sub_admin":
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        contract = db.get_contract_info(user.get("contract_id"), contract_table)
        logger.debug(contract)
        device_list = contract.get("contract_data", {}).get("device_list", {})
        logger.debug(device_list)
        notification_list = []
        for device_id in device_list:
            device = db.get_device_info(device_id, device_table)
            logger.info({"device": device})
            if not device:
                continue
            notification_settings = (
                device.get("device_data", {}).get("config", {}).get("notification_settings", {})
            )
            logger.debug(notification_settings)
            for notification_setting in notification_settings:
                notification_list.append(
                    {
                        "device_id": device_id,
                        "device_name": device.get("device_data", {})
                        .get("config", {})
                        .get("device_name", ""),
                        "device_imei": device.get("imei", ""),
                        "event_trigger": notification_setting.get("device_state", ""),
                        "terminal_no": notification_setting.get("terminal_no", ""),
                        "event_type": notification_setting.get("event_type", ""),
                        "change_detail": notification_setting.get("change_detail", ""),
                        "notification_target_list": notification_setting.get(
                            "notification_target_list", []
                        ),
                    }
                )

        res_body = {
            "message": "",
            "notification_list": notification_list,
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
