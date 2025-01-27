import json
import os
import validate

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from collections import OrderedDict

import auth
import db
import ssm
import convert

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user()
def lambda_handler(event, context, user):
    res_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }
    logger.info({"user": user})

    # DynamoDB操作オブジェクト生成
    try:
        tables = {
            "device_table": dynamodb.Table(ssm.table_names["DEVICE_TABLE"]),
            "contract_table": dynamodb.Table(ssm.table_names["CONTRACT_TABLE"]),
            "device_relation_table": dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"]),
            "group_table": dynamodb.Table(ssm.table_names["GROUP_TABLE"]),
            "account_table": dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"]),
            "user_table": dynamodb.Table(ssm.table_names["USER_TABLE"])
        }
    except KeyError as e:
        body = {"message": e}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }

    try:
        # パラメータチェック
        validate_result = validate.validate(event, user, tables)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        # デバイス情報取得
        device_id = validate_result["device_id"]
        logger.debug(device_id)
        device = db.get_device_info_other_than_unavailable(device_id, tables["device_table"])
        logger.info({"device": device})

        group_id_list = db.get_device_relation_group_id_list(
            device_id, tables["device_relation_table"]
        )
        logger.debug(f"group_id_list: {group_id_list}")
        unique_group_id_list = list(OrderedDict.fromkeys(group_id_list)) if group_id_list else []
        logger.debug(f"unique_group_id_list: {unique_group_id_list}")
        group_name_list = []
        for group_id in unique_group_id_list:
            group_info = db.get_group_info(group_id, tables["group_table"])
            logger.debug(f"group_info: {group_info}")
            group_name_list.append(
                group_info.get("group_data", {}).get("config", {}).get("group_name")
            )
        if group_name_list:
            group_name_list.sort()

        notification_settings = device.get("device_data", {}).get("config", {}).get("notification_settings", {})
        notification_target_list = device.get("device_data", []).get("config", []).get("notification_target_list", [])
        contract_info = db.get_contract_info(user.get("contract_id"), tables["contract_table"])
        contract_user_id_list = contract_info.get("contract_data", {}).get("user_list", [])
        notification_target_list = list(set(notification_target_list) & set(contract_user_id_list))
        logger.debug(notification_settings)
        notification_list = []
        for notification_setting in notification_settings:
            notification_list.append(
                {
                    "event_trigger": notification_setting.get("event_trigger", ""),
                    "terminal_no": notification_setting.get("terminal_no", ""),
                    "event_type": notification_setting.get("event_type", ""),
                    "change_detail": notification_setting.get("change_detail", ""),
                    "custom_event_id": notification_setting.get("custom_event_id", ""),
                }
            )
        notification_target_object_list = []
        for notification_target in notification_target_list:
            user_info = db.get_user_info_by_user_id(notification_target, tables["user_table"])
            account_info = db.get_account_info_by_account_id(user_info.get("account_id"), tables["account_table"])
            send_info = {
                "user_id": notification_target,
                "user_name": account_info.get("user_data").get("config").get("user_name"),
                "mail_address": account_info.get("email_address")
            }
            notification_target_object_list.append(send_info)

        res_body = {
            "message": "",
            "device_id": device_id,
            "device_name": device.get("device_data", {}).get("config", {}).get("device_name", ""),
            "group_name_list": group_name_list,
            "device_imei": device.get("imei", ""),
            "device_sigfox_id": device.get("sigfox_id", ""),
            "device_code": device.get("device_data", {}).get("param", {}).get("device_code", ""),
            "notification_list": notification_list,
            "notification_target_list": notification_target_object_list,
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
