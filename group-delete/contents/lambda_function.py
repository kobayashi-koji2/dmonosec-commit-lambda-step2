import json
import traceback
import os

from aws_lambda_powertools import Logger
import boto3
import ssm

import convert
import validate
import ddb

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
parameter = None
logger = Logger()

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
        logger.info(validate_result)
        if validate_result["code"] != "0000":
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
            parameter.get("CONTRACT_TABLE"),
            parameter.get("GROUP_TABLE"),
            parameter.get("DEVICE_RELATION_TABLE"),
        )

        if transact_result:
            res_body = {"code": "0000", "message": "グループの削除が完了しました。"}
        else:
            res_body = {"code": "9999", "message": "グループの削除に失敗しました。"}

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
