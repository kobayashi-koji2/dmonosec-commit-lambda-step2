import json
import boto3
import validate
import generate_detail
import os
import ddb
from botocore.exceptions import ClientError
import traceback
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import db
import convert
import ssm

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
            tables = {
                "user_table": dynamodb.Table(ssm.table_names["USER_TABLE"]),
                "device_table": dynamodb.Table(ssm.table_names["DEVICE_TABLE"]),
                "group_table": dynamodb.Table(ssm.table_names["GROUP_TABLE"]),
                "device_state_table": dynamodb.Table(ssm.table_names["STATE_TABLE"]),
                "contract_table": dynamodb.Table(ssm.table_names["CONTRACT_TABLE"]),
                "device_relation_table": dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"]),
                "automation_table": dynamodb.Table(ssm.table_names["AUTOMATION_TABLE"])  # TODO 連動制御管理テーブル追加時に変更の可能性あり
            }
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェックおよびimei取得
        validate_result = validate.validate(event, user_info, tables)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }
        # デバイス設定更新
        body = validate_result["body"]
        device_id = validate_result["device_id"]
        imei = validate_result["imei"]
        convert_param = convert.float_to_decimal(body)
        logger.info(f"デバイスID:{device_id}")
        logger.info(f"IMEI:{imei}")
        try:
            ddb.update_device_settings(device_id, imei, convert_param, tables["device_table"])
        except ClientError as e:
            logger.info(f"デバイス設定更新エラー e={e}")
            res_body = {"message": "デバイス設定の更新に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(
                    res_body, ensure_ascii=False, default=convert.decimal_default_proc
                ),
            }

        # 連動制御情報更新
        result = ddb.sync_automation_info_list(device_id, convert_param, tables["automation_table"])
        if not result:
            logger.info("連動制御情報更新エラー")
            res_body = {"message": "連動制御情報の更新に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(
                    res_body, ensure_ascii=False, default=convert.decimal_default_proc
                ),
            }

        # デバイス情報取得
        try:
            # デバイス設定取得
            device_info = ddb.get_device_info(device_id, tables["device_table"]).get("Items", {})
            # デバイス現状態取得
            device_state = db.get_device_state(device_id, tables["device_state_table"])
            # グループ情報取得
            group_id_list = db.get_device_relation_group_id_list(
                device_id, tables["device_relation_table"]
            )
            group_info_list = []
            for group_id in group_id_list:
                group_info = db.get_group_info(group_id, tables["group_table"])
                if group_info:
                    group_info_list.append(group_info)
            # 連動制御情報取得
            automation_info_list = ddb.get_automation_info_list(device_id, tables["automation_table"]).get("Items", [])
            # デバイス詳細情報生成
            res_body = generate_detail.get_device_detail(device_info[0], device_state, group_info_list, automation_info_list)

        except ClientError as e:
            logger.info(e)
            body = {"message": "デバイス詳細の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        logger.info(f"レスポンス:{res_body}")
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
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
