import json
import os
import boto3
import traceback

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

import ssm
import db
import convert

import validate

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

parameter = None
logger = Logger()


def lambda_handler(event, context):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # コールドスタートの場合パラメータストアから値を取得してグローバル変数にキャッシュ
        global parameter
        if not parameter:
            logger.info("try ssm get parameter")
            response = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(response)
            logger.info("tried ssm get parameter")
        else:
            logger.info("passed ssm get parameter")
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(parameter["USER_TABLE"])
            account_table = dynamodb.Table(parameter.get("ACCOUNT_TABLE"))
            contract_table = dynamodb.Table(parameter.get("CONTRACT_TABLE"))
            group_table = dynamodb.Table(parameter.get("GROUP_TABLE"))
            device_table = dynamodb.Table(parameter.get("DEVICE_TABLE"))
            device_relation_table = dynamodb.Table(parameter.get("DEVICE_RELATION_TABLE"))
        except KeyError as e:
            parameter = None
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
        user = validate_result["user_info"]

        device_list = []
        try:
            contract = validate_result["contract_info"]
            group_id = validate_result["request_params"]["group_id"]
            group_info = db.get_group_info(group_id, group_table).get("Item", {})
            relation_list = db.get_device_relation(
                "g-" + group_id, device_relation_table, sk_prefix="d-"
            )
            for relation in relation_list:
                device_id = relation["key2"][2:]
                logger.info(device_id)
                logger.info(db.get_device_info(device_id, device_table))
                device_info = db.get_device_info(device_id, device_table)
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
            body = {"code": "9999", "message": "グループ一覧の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        res_body = {
            "code": "0000",
            "message": "",
            "group_id": group_info.get("group_id", {}),
            "group_name": group_info.get("group_data", {}).get("config", {}).get("group_name", {}),
            "device_list": device_list,
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc)
            #'body':res_body
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
