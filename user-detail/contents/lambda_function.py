import json
import os
import time

import traceback
import boto3

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from botocore.exceptions import ClientError

import auth
import ssm
import db
import convert

import validate

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


@auth.verify_login_user()
def lambda_handler(event, context, user):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(event, user, contract_table)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        user_id = validate_result["request_params"]["user_id"]
        user_info = db.get_user_info_by_user_id(user_id, user_table)
        account = db.get_account_info_by_account_id(user_info["account_id"], account_table)
        account_config = account.get("user_data", {}).get("config", {})

        auth_status = account_config.get("auth_status")
        if auth_status == "unauthenticated":
            if account_config.get("auth_period", 0) / 1000 < int(time.time()):
                auth_status = "expired"

        group_id_list = db.get_user_relation_group_id_list(user_id, device_relation_table)
        group_list = []
        device_list = []
        for group_id in group_id_list:
            group_info = db.get_group_info(group_id, group_table)
            logger.info(group_info)
            group_list.append(
                {
                    "group_id": group_id,
                    "group_name": group_info.get("group_data", {})
                    .get("config", {})
                    .get("group_name"),
                }
            )

            group_device_id_list = db.get_group_relation_device_id_list(group_id, device_relation_table)
            for group_device_id in group_device_id_list:
                logger.info(group_device_id)
                device_info = db.get_device_info_other_than_unavailable(group_device_id, device_table)
                logger.info(device_info)
                if not device_info:
                    continue
                device_list.append(
                    {
                        "device_id": group_device_id,
                        "device_name": device_info.get("device_data", {})
                        .get("config", {})
                        .get("device_name", {}),
                    }
                )

        if group_list:
            group_list = sorted(group_list, key=lambda x:x['group_name'])

        device_id_list = db.get_user_relation_device_id_list(
            user_id, device_relation_table, include_group_relation=False
        )
        for device_id in device_id_list:
            device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
            if device_info is not None:
                device_list.append(
                    {
                        "device_id": device_id,
                        "device_name": device_info.get("device_data", {})
                        .get("config", {})
                        .get("device_name"),
                    }
                )

        unique_devices = {device['device_id']: device for device in device_list}
        device_list = list(unique_devices.values())

        res_body = {
            "message": "",
            "user_id": user_id,
            "email_address": account.get("email_address"),
            "user_name": account_config.get("user_name"),
            "user_type": user_info.get("user_type"),
            "auth_status": auth_status,
            "mfa_flag": account_config.get("mfa_flag"),
            "management_group_list": group_list,
            "management_device_list": device_list,
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        logger.error(traceback.format_exc())
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
