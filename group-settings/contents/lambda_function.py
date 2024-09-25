import json
import os
from decimal import Decimal

from aws_lambda_powertools import Logger
import ssm
import boto3
from botocore.exceptions import ClientError
from aws_xray_sdk.core import patch_all

import auth
import convert
import db
import validate
import group

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]


@auth.verify_login_user()
def lambda_handler(event, context, user_info):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            pre_register_table = dynamodb.Table(ssm.table_names["PRE_REGISTER_DEVICE_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(event, user_info, contract_table, group_table)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }
        group_info = validate_result["request_params"]

        logger.info(event["httpMethod"])

        # グループ新規登録
        if event["httpMethod"] == "POST":
            result = group.create_group_info(
                group_info,
                validate_result["contract_info"],
                ssm.table_names["GROUP_TABLE"],
                ssm.table_names["CONTRACT_TABLE"],
                ssm.table_names["DEVICE_RELATION_TABLE"],
            )
        # グループ更新
        elif event["httpMethod"] == "PUT":
            group_id = event["pathParameters"]["group_id"]
            result = group.update_group_info(
                group_info,
                group_id,
                device_relation_table,
                device_table,
                ssm.table_names["GROUP_TABLE"],
                ssm.table_names["DEVICE_RELATION_TABLE"],
                ssm.table_names["DEVICE_TABLE"],
            )

        if not result[0]:
            res_body = {"message": "グループの登録・更新に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(
                    res_body, ensure_ascii=False, default=convert.decimal_default_proc
                ),
            }

        group_info = db.get_group_info(result[1], group_table)
        device_id_list = db.get_group_relation_device_id_list(result[1], device_relation_table)
        logger.info(result[1])
        logger.info(device_id_list)
        device_list = []
        for device_id in device_id_list:
            logger.info(device_id)
            device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
            if not device_info:
                continue
            device_list.append(
                {
                    "device_id": device_id,
                    "device_name": device_info.get("device_data", {})
                    .get("config", {})
                    .get("device_name", {}),
                }
            )
            
        unregistered_device_list = []    
        unregistered_device_id_list = db.get_group_relation_pre_register_device_id_list(group_info["group_id"], device_relation_table)
        for unregistered_device_id in unregistered_device_id_list:
            logger.info(f"unregistered_device_id:{unregistered_device_id}")
            unregistered_device_info = db.get_device_info_by_imei(unregistered_device_id, pre_register_table)
            if not unregistered_device_info:
                continue
            unregistered_device_list.append(
                {
                    "device_imei": unregistered_device_id,
                    "device_code": unregistered_device_info.get("device_code", {})
                }
            )
            
        res_body = {
            "message": "",
            "group_id": group_info["group_id"],
            "group_name": group_info.get("group_data", {}).get("config", {}).get("group_name", {}),
            "device_list": device_list,
            "unregistered_device_list": unregistered_device_list,
        }

        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
