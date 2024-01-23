import json
import os

import boto3
from aws_lambda_powertools import Logger

import auth
import db
import ssm

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
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
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
        contract_device_list = contract.get("contract_data", {}).get("device_list", {})
        device_list = []
        for device_id in contract_device_list:
            device = db.get_device_info(device_id, device_table)
            user_id_list = db.get_device_relation_user_id_list(device_id, device_relation_table)
            device_list.append(
                {
                    "device_id": device_id,
                    "device_name": device.get("device_data", {})
                    .get("config", {})
                    .get("device_name", ""),
                    "device_imei": device.get("imei", ""),
                    "user_list": user_id_list,
                }
            )

        res_body = {
            "message": "",
            "device_list": device_list,
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
