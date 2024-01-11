import json
import os
import traceback
from decimal import Decimal
import time

from aws_lambda_powertools import Logger
import boto3
from botocore.exceptions import ClientError

import ssm
import convert
import ddb
import validate

parameter = None
logger = Logger()
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
            logger.info("try ssm get parameter")
            response = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(response)
            logger.info("tried ssm get parameter")
        else:
            logger.info("passed ssm get parameter")
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(parameter["USER_TABLE"])
            contract_table = dynamodb.Table(parameter.get("CONTRACT_TABLE"))
            device_relation_table = dynamodb.Table(parameter.get("DEVICE_RELATION_TABLE"))
            remote_controls_table = dynamodb.Table(parameter.get("REMOTE_CONTROL_TABLE"))
        except KeyError as e:
            parameter = None
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        # パラメータチェック
        validate_result = validate.validate(
            event,
            contract_table,
            user_table,
            device_relation_table,
            remote_controls_table,
        )
        if validate_result["code"] != "0000":
            return {
                "statusCode": 200,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        remote_control = validate_result["remote_control"]
        req_datetime = remote_control["req_datetime"]
        limit_datetime = req_datetime + 10000  # 10秒
        while not remote_control.get("control_result") and time.time() <= limit_datetime / 1000:
            time.sleep(1)
            logger.info(time.time())
            remote_control = ddb.get_remote_control_info(
                remote_control["device_req_no"], remote_controls_table
            )

        control_result = "0" if remote_control.get("control_result") == 0 else "1"
        logger.info(f"result:{control_result}")

        res_body = {
            "code": "0000",
            "message": "",
            "device_req_no": validate_result["request_params"]["device_req_no"],
            "control_result": control_result,
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
