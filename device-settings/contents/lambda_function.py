import json
import boto3
import validate
import generate_detail
import os
import ddb
import time
from datetime import datetime
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
                "device_table": dynamodb.Table(ssm.table_names["DEVICE_TABLE"]),
                "group_table": dynamodb.Table(ssm.table_names["GROUP_TABLE"]),
                "device_state_table": dynamodb.Table(ssm.table_names["STATE_TABLE"]),
                "contract_table": dynamodb.Table(ssm.table_names["CONTRACT_TABLE"]),
                "device_relation_table": dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"]),
            }
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェックおよびidentificaion_id(IMEI,sigfox_id)取得
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
        sigfox_id = validate_result["sigfox_id"]
        convert_param = convert.float_to_decimal(body)
        logger.info(f"デバイスID:{device_id}")
        logger.info(f"IMEI:{imei}")
        logger.info(f"sigfox_id:{sigfox_id}")
        identification_id = imei if imei else sigfox_id
        try:
            ddb.update_device_settings(device_id, identification_id, convert_param, tables["device_table"])
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

        # デバイス情報取得
        try:
            # デバイス設定取得
            device_info = ddb.get_device_info(device_id, tables["device_table"])
            # デバイス現状態取得
            device_state = db.get_device_state(device_id, tables["device_state_table"])

            if device_state and device_state.get("device_healthy_state", 0) == 1:
                if device_info.get("device_type") == "UnaTag":
                    last_recv_datetime = device_state.get("unatag_last_recv_datetime")
                else:
                    last_recv_datetime = device_state.get("device_abnormality_last_update_datetime")

                # デバイスヘルシーチェック
                now = datetime.now()
                now_datetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)
                device_healthy_period = device_info["device_data"]["config"].get("device_healthy_period", 0)
                elapsed_time = now_datetime - last_recv_datetime
                device_healthy_period_time = device_healthy_period * 24 * 60 * 60 * 1000

                if elapsed_time < device_healthy_period_time:
                    try:
                        ddb.update_device_state(device_id, tables["device_state_table"])
                    except ClientError as e:
                        logger.info(f"デバイス現状態更新エラー e={e}")
                        res_body = {"message": "デバイス現状態の更新に失敗しました。"}
                        return {
                            "statusCode": 500,
                            "headers": res_headers,
                            "body": json.dumps(
                                res_body, ensure_ascii=False, default=convert.decimal_default_proc
                            ),
                        }

                    # 最新のデバイス現状態を再取得
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
            if group_info_list:
                group_info_list = sorted(group_info_list, key=lambda x:x['group_data']['config']['group_name'])
            # デバイス詳細情報生成
            res_body = generate_detail.get_device_detail(
                device_info, device_state, group_info_list
            )

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
