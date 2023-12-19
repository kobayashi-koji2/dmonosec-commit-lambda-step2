import json
import os
import logging
import traceback
from decimal import Decimal

import ssm
import boto3
from botocore.exceptions import ClientError

import convert
import db
import validate
import group

parameter = None
logger = logging.getLogger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]


def lambda_handler(event, context):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # コールドスタートの場合パラメータストアから値を取得してグローバル変数にキャッシュ
        global parameter
        if not parameter:
            print("try ssm get parameter")
            response = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(response)
            print("tried ssm get parameter")
        else:
            print("passed ssm get parameter")
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(parameter["USER_TABLE"])
            device_table = dynamodb.Table(parameter.get("DEVICE_TABLE"))
            group_table = dynamodb.Table(parameter.get("GROUP_TABLE"))
            contract_table = dynamodb.Table(parameter.get("CONTRACT_TABLE"))
            device_relation_table = dynamodb.Table(
                parameter.get("DEVICE_RELATION_TABLE")
            )
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
        group_info = validate_result["request_params"]

        print(event["httpMethod"])

        # グループ新規登録
        if event["httpMethod"] == "POST":
            result = group.create_group_info(
                group_info,
                validate_result["contract_info"],
                parameter.get("GROUP_TABLE"),
                parameter.get("CONTRACT_TABLE"),
                parameter.get("DEVICE_RELATION_TABLE"),
            )
        # グループ更新
        elif event["httpMethod"] == "PUT":
            group_id = event["pathParameters"]["group_id"]
            result = group.update_group_info(
                group_info,
                group_id,
                device_relation_table,
                parameter.get("GROUP_TABLE"),
                parameter.get("DEVICE_RELATION_TABLE"),
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

        group_info = db.get_group_info(result[1], group_table).get("Item", {})
        relation_list = db.get_device_relation(
            "g-" + result[1], device_relation_table, sk_prefix="d-"
        )
        print(result[1])
        print(relation_list)
        device_list = []
        for relation in relation_list:
            device_id = relation["key2"][2:]
            print(device_id)
            print(db.get_device_info(device_id, device_table))
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
        res_body = {
            "code": "0000",
            "message": "",
            "group_id": group_info["group_id"],
            "group_name": group_info.get("group_data", {})
            .get("config", {})
            .get("group_name", {}),
            "device_list": device_list,
        }

        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(
                res_body, ensure_ascii=False, default=convert.decimal_default_proc
            ),
        }
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        res_body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(
                res_body, ensure_ascii=False, default=convert.decimal_default_proc
            ),
        }
