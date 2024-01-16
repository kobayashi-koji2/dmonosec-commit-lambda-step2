import json
import os
import boto3
import traceback

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

import auth
import ssm
import db
import convert

import validate

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


def lambda_handler(event, context):
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

        try:
            user_info = auth.verify_user(event, user_table)
        except auth.AuthError as e:
            logger.info("ユーザー検証失敗", exc_info=True)
            return {
                "statusCode": e.code,
                "headers": res_headers,
                "body": json.dumps({"message": e.message}, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(event, user_info, contract_table)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        device_list = []
        try:
            contract = validate_result["contract_info"]
            group_id = validate_result["request_params"]["group_id"]
            group_info = db.get_group_info(group_id, group_table)
            device_id_list = db.get_group_relation_device_id_list(group_id, device_relation_table)
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
        except ClientError as e:
            logger.info(e)
            logger.info(traceback.format_exc())
            body = {"message": "グループ一覧の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        res_body = {
            "message": "",
            "group_id": group_info.get("group_id", {}),
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
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
