import json
import boto3
import validate
import generate_detail
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
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
                
        body_params = json.loads(event.get("body", "{}"))
        logger.info(body_params)
                
        # パラメータチェック
        validate_result = validate.validate(event, user, device_table, contract_table)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }
        logger.info(user)
        custom_event_info = validate_result["custom_info"]
        device_id = validate_result["device_id"]
        device_info = validate_result["device_info"]
        
        # カスタムイベント新規登録
        if event.get("httpMethod") == "POST":
            logger.info("カスタムイベント新規登録")
            result = generate_detail.create_custom_event_info(
                custom_event_info,
                device_table,
                device_id,
                device_info,
            )
            logger.info(result)

            if not result[0]:
                logger.info(result[0])
                res_body = {"message": "イベントカスタムの作成に失敗しました。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(result, ensure_ascii=False),
                }
        # カスタムイベント設定更新
        if event.get("httpMethod") == "PUT":
            logger.info("カスタムイベント設定更新")
            result = generate_detail.update_custom_event_info(
                custom_event_info,
                device_table,
                device_id,
                device_info,
            )
            logger.info(result)

            if not result[0]:
                logger.info(result[0])
                res_body = {"message": "イベントカスタムの更新に失敗しました。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(result, ensure_ascii=False),
                }
                
        ### 7. カスタムイベント設定情報取得
        device_info = ddb.get_device_info(device_id, device_table)
        custom_event_list = list()
        for item in device_info:
            for custom_event_a in item["device_data"]["config"]["custom_event_list"]:
                ### 8. メッセージ応答
                logger.info(custom_event_a)
                if custom_event_a["event_type"] == 0:
                    custom_event_item = {
                        "custom_event_id": custom_event_a["custom_event_id"],
                        'custom_event_reg_datetime': custom_event_a["custom_event_reg_datetime"],
                        "event_type": custom_event_a["event_type"],
                        "custom_event_name": custom_event_a["custom_event_name"],
                        "time": custom_event_a["time"],
                        "weekday": custom_event_a["weekday"],
                        "di_event_list": custom_event_a["di_event_list"],
                    }
                if custom_event_a["event_type"] == 1:
                    custom_event_item = {
                        "custom_event_id": custom_event_a["custom_event_id"],
                        'custom_event_reg_datetime': custom_event_a["custom_event_reg_datetime"],
                        "event_type": custom_event_a["event_type"],
                        "custom_event_name": custom_event_a["custom_event_name"],
                        "elapsed_time": custom_event_a["elapsed_time"],
                        "di_event_list": custom_event_a["di_event_list"],
                    }
                custom_event_list.append(custom_event_item)

        res_body = {"message": "", "custom_event_list": custom_event_list}
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
