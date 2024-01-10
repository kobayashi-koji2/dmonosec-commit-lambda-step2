import json
import logging
import traceback
import os

import boto3
import ssm

import convert
import validate
import ddb

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
parameter = None

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]


def lambda_handler(event, context):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(
                ssm.table_names["DEVICE_RELATION_TABLE"]
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
        print(validate_result)
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
            ssm.table_names["CONTRACT_TABLE"],
            ssm.table_names["GROUP_TABLE"],
            ssm.table_names["DEVICE_RELATION_TABLE"],
        )

        if transact_result:
            res_body = {"code": "0000", "message": "グループの削除が完了しました。"}
        else:
            res_body = {"code": "9999", "message": "グループの削除に失敗しました。"}

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
