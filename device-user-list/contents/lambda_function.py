import json
import os

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

import auth
import db
import ssm

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
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
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
        pathParam = event.get("pathParameters", {})
        if not pathParam or "device_id" not in pathParam:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "パラメータが不正です。"}, ensure_ascii=False),
            }
        device_id = pathParam["device_id"]

        # ユーザー権限チェック
        if user.get("user_type") != "admin" and user.get("user_type") != "sub_admin":
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        # デバイスIDが契約に紐づいているかチェック
        contract = db.get_contract_info(user.get("contract_id"), contract_table)
        contract_device_list = contract.get("contract_data", {}).get("device_list", {})
        if device_id not in contract_device_list:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "不正なデバイスIDが指定されています。"}, ensure_ascii=False),
            }

        # ユーザー一覧生成
        user_list = []
        admin_user_id_list = db.get_admin_user_id_list(user.get("contract_id"), user_table)
        logger.debug(f"admin_user_id_list: {admin_user_id_list}")
        worker_user_id_list = db.get_device_relation_user_id_list(device_id, device_relation_table)
        logger.debug(f"worker_user_id_list: {worker_user_id_list}")
        user_id_list = admin_user_id_list + worker_user_id_list
        logger.debug(f"user_id_list: {user_id_list}")
        for user_id in user_id_list:
            logger.debug(user_id)
            user_info = db.get_user_info_by_user_id(user_id, user_table)
            account_info = db.get_account_info_by_account_id(
                user_info.get("account_id"), account_table
            )
            user_list.append(
                {
                    "user_id": user_id,
                    "user_name": account_info.get("user_data", {}).get("config", {}).get("user_name", ""),
                    "email_address": account_info.get("email_address", ""),
                }
            )

        res_body = {
            "message": "",
            "user_list": user_list,
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
