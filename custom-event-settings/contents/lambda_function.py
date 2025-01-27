import json
import boto3
import validate
import generate_detail
import db
import os
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
        custom_event_info = validate_result["custom_info"]
        device_id = validate_result["device_id"]
        
        # カスタムイベント新規登録
        if event.get("httpMethod") == "POST":
            result = generate_detail.create_custom_event_info(
                custom_event_info,
                device_table,
                device_id,
                device_state_table,
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
            result = generate_detail.update_custom_event_info(
                custom_event_info,
                device_table,
                device_id,
                device_state_table,
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
        device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
        custom_event_list = list()
        for custom_event in device_info.get("device_data").get("config").get("custom_event_list", []):
            ### 8. メッセージ応答
            if custom_event["event_type"] == 0:
                custom_event_item = {
                    "custom_event_id": custom_event["custom_event_id"],
                    'custom_event_reg_datetime': custom_event["custom_event_reg_datetime"],
                    "event_type": custom_event["event_type"],
                    "custom_event_name": custom_event["custom_event_name"],
                    "time": custom_event["time"],
                    "weekday": custom_event["weekday"],
                    "di_event_list": custom_event["di_event_list"],
                }
            if custom_event["event_type"] == 1:
                custom_event_item = {
                    "custom_event_id": custom_event["custom_event_id"],
                    'custom_event_reg_datetime': custom_event["custom_event_reg_datetime"],
                    "event_type": custom_event["event_type"],
                    "custom_event_name": custom_event["custom_event_name"],
                    "elapsed_time": custom_event["elapsed_time"],
                    "di_event_list": custom_event["di_event_list"],
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
