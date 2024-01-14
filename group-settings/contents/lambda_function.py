import json
import os
import traceback
from decimal import Decimal

from aws_lambda_powertools import Logger
import ssm
import boto3
from botocore.exceptions import ClientError

import convert
import db
import validate
import group

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]


def lambda_handler(event, context):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        except KeyError as e:
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        # パラメータチェック
        validate_result = validate.validate(event, contract_table, user_table)
        if validate_result["code"] != "0000":
            return {
                "statusCode": 200,
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
                ssm.table_names["GROUP_TABLE"],
                ssm.table_names["DEVICE_RELATION_TABLE"],
            )

        if not result[0]:
            res_body = {"code": "9999", "message": "グループの登録・更新に失敗しました。"}
            return {
                "statusCode": 200,
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
            device_info = db.get_device_info(device_id, device_table)
            logger.info(device_info)
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
        res_body = {
            "code": "0000",
            "message": "",
            "group_id": group_info["group_id"],
            "group_name": group_info.get("group_data", {}).get("config", {}).get("group_name", {}),
            "device_list": device_list,
        }

        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
