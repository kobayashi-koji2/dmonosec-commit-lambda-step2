import json
import traceback
import os

from aws_lambda_powertools import Logger
import boto3
from aws_xray_sdk.core import patch_all

import auth
import ssm
import convert
import validate
import ddb

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
logger = Logger()

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
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(event, user_info, contract_table)
        logger.info(validate_result)
        if validate_result.get("message"):
            return {
                "statusCode": 200,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        # グループ削除
        transact_result = ddb.delete_group_info(
            validate_result["request_params"]["group_id"],
            validate_result["contract_info"],
            device_relation_table,
            device_table,
            ssm.table_names["CONTRACT_TABLE"],
            ssm.table_names["GROUP_TABLE"],
            ssm.table_names["DEVICE_RELATION_TABLE"],
            ssm.table_names["DEVICE_TABLE"],
        )

        if not transact_result:
            res_body = {"message": "グループの削除に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(
                    res_body, ensure_ascii=False, default=convert.decimal_default_proc
                ),
            }

        return {
            "statusCode": 204,
            "headers": res_headers,
        }

    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
