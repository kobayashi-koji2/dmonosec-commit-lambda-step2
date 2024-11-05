import json
import boto3
import validate
import ddb
import os
from botocore.exceptions import ClientError
import traceback
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import db
import ssm
import convert
import auth

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


@auth.verify_login_user()
def lambda_handler(event, context, user):

    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_state_table = dynamodb.Table(ssm.table_names["STATE_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
                
        # パラメータチェック
        validate_result = validate.validate(event, user, device_table, contract_table)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }
        logger.info(user)
        custom_event_id = validate_result["custom_event_id"]
        device_id = validate_result["device_id"]
        identification_id = validate_result["identification_id"]
        
        # カスタムイベント設定削除
        custom_event_delete = ddb.delete_custom_event(device_table, custom_event_id, device_id, identification_id)
        custom_event_delete_in_state_table = ddb.delete_custom_event_in_state_table(device_state_table, custom_event_id, device_id)
        logger.info(custom_event_delete)
        logger.info(custom_event_delete_in_state_table)
        
        ### 7. メッセージ応答
        res_body = {"message": ""}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
            
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
