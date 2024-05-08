import json
import os

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
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
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
        device_id_list = contract.get("contract_data", {}).get("device_list", {})
        logger.debug(device_id_list)
        device_list = []
        for device_id in device_id_list:
            device = db.get_device_info_other_than_unavailable(device_id, device_table)
            logger.info({"device": device})
            if not device:
                continue
            notification_settings = (
                device.get("device_data", {}).get("config", {}).get("notification_settings", {})
            )
            if not notification_settings:
                continue

            group_id_list = db.get_device_relation_group_id_list(
                device_id, device_relation_table
            )
            logger.debug(f"group_id_list: {group_id_list}")
            unique_group_id_list = list(OrderedDict.fromkeys(group_id_list)) if group_id_list else []
            logger.debug(f"unique_group_id_list: {unique_group_id_list}")
            group_name_list = []
            for group_id in unique_group_id_list:
                group_info = db.get_group_info(group_id, group_table)
                logger.debug(f"group_info: {group_info}")
                group_name_list.append(
                    group_info.get("group_data", {}).get("config", {}).get("group_name")
                )
            if group_name_list:
                group_name_list.sort()

            device_list.append(
                {
                    "device_id": device_id,
                    "device_name": device.get("device_data", {}).get("config", {}).get("device_name", ""),
                    "group_name_list": group_name_list,
                    "device_imei": device.get("imei", "")
                }
            )

        res_body = {
            "message": "",
            "device_list": device_list,
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
