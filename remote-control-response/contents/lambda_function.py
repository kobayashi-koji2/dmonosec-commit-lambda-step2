import json
import os
import traceback
from decimal import Decimal
import time

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
import boto3
from botocore.exceptions import ClientError

import auth
import ssm
import convert
import ddb
import db
import validate

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
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(
                ssm.table_names["DEVICE_RELATION_TABLE"]
            )
            remote_controls_table = dynamodb.Table(
                ssm.table_names["REMOTE_CONTROL_TABLE"]
            )
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(
            event,
            user_info,
            contract_table,
            device_relation_table,
            remote_controls_table,
            device_table,
        )
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        remote_control = validate_result["remote_control"]
        req_datetime = remote_control["req_datetime"]
        limit_datetime = req_datetime + 10000  # 10秒
        while (
            not remote_control.get("control_result")
            and time.time() <= limit_datetime / 1000
        ):
            time.sleep(1)
            logger.info(time.time())
            remote_control = ddb.get_remote_control_info(
                remote_control["device_req_no"], remote_controls_table
            )

        control_result = "0" if remote_control.get("control_result") == "0" else "1"
        logger.info(f"result:{control_result}")

        res_body = {
            "message": "",
            "device_req_no": validate_result["request_params"]["device_req_no"],
            "control_result": control_result,
        }

        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(
                res_body, ensure_ascii=False, default=convert.decimal_default_proc
            ),
        }
    except Exception as e:
        logger.info(e)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(
                res_body, ensure_ascii=False, default=convert.decimal_default_proc
            ),
        }
