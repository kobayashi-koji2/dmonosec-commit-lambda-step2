import json
import os
import boto3
import traceback

from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

import ssm
import convert
import db
import ddb
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

        # ユーザ新規登録
        if event["httpMethod"] == "POST":
            result = ddb.create_user_info(
                validate_result["request_params"],
                "contract-dummy",  # TODO ログイン時の契約IDを指定
                validate_result["contract_info"],
                account_table,
                ssm.table_names["ACCOUNT_TABLE"],
                ssm.table_names["USER_TABLE"],
                ssm.table_names["CONTRACT_TABLE"],
                ssm.table_names["DEVICE_RELATION_TABLE"],
            )
            # TODO 招待メール送信？

        # ユーザ更新
        elif event["httpMethod"] == "PUT":
            result = ddb.update_user_info(
                validate_result["request_params"],
                account_table,
                user_table,
                device_relation_table,
                ssm.table_names["ACCOUNT_TABLE"],
                ssm.table_names["USER_TABLE"],
                ssm.table_names["DEVICE_RELATION_TABLE"],
            )

        if not result[0]:
            logger.info(result[0])
            res_body = {"code": "9999", "message": "ユーザの登録・更新に失敗しました。"}
            return {
                "statusCode": 200,
                "headers": res_headers,
                "body": json.dumps(
                    res_body, ensure_ascii=False, default=convert.decimal_default_proc
                ),
            }

        # レスポンス用データ取得
        user_id = result[1]
        account_info = db.get_account_info_by_account_id(user.get("account_id"), account_table)
        account = account_info["Item"]
        account_config = account.get("user_data", {}).get("config", {})

        group_relation_list = db.get_device_relation(
            "u-" + user_id, device_relation_table, sk_prefix="g-"
        )
        logger.info(group_relation_list)
        group_list = []
        for group_relation in group_relation_list:
            logger.info(group_relation)
            group_id = group_relation["key2"][2:]
            logger.info(group_id)
            group_info = db.get_group_info(group_id, group_table).get("Item")
            logger.info(group_info)
            group_list.append(
                {
                    "group_id": group_id,
                    "group_name": group_info.get("group_data", {})
                    .get("config", {})
                    .get("group_name"),
                }
            )

        device_relation_list = db.get_device_relation(
            "u-" + user_id, device_relation_table, sk_prefix="d-"
        )
        device_list = []
        for device_relation in device_relation_list:
            device_id = device_relation["key2"][2:]
            device_info = db.get_device_info(device_id, device_table)
            device_list.append(
                {
                    "device_id": device_id,
                    "device_name": device_info.get("device_data", {})
                    .get("config", {})
                    .get("device_name"),
                }
            )

        # レスポンス作成
        res_body = {
            "code": "0000",
            "message": "",
            "user_id": user_id,
            "email_address": account.get("email_address"),
            "user_name": account_config.get("user_name"),
            "user_type": user.get("user_type"),
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
        logger.info(traceback.format_exc())
        body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
