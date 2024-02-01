import json
import boto3
import validate
import os
import re
import ddb
from botocore.exceptions import ClientError
import traceback
from decimal import Decimal
from aws_lambda_powertools import Logger

# layer
import auth
import db
import convert
import ssm

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

        # パラメータチェック
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
        imei = body["device_imei"]
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
        else:
            # デバイス設定取得
            device_info = ddb.get_device_info_by_id_imei(device_id, imei, tables["device_table"])[
                "Item"
            ]
            device_info_param = device_info.get("device_data", {}).get("param", {})
            device_info_config = device_info.get("device_data", {}).get("config", {})

            # グループ情報取得
            group_id_list = db.get_device_relation_group_id_list(
                device_id, tables["device_relation_table"]
            )
            group_list = []
            for gruop_id in group_id_list:
                group_info = db.get_group_info(gruop_id, tables["group_table"])
                if group_info:
                    group_list.append(
                        {
                            "group_id": group_info["group_id"],
                            "group_name": group_info["group_data"]["config"]["group_name"],
                        }
                    )

        res_body = num_to_str(
            {
                "message": "",
                "device_id": device_info["device_id"],
                "device_name": device_info_config.get("device_name", ""),
                "device_code": device_info_param.get("device_code", ""),
                "device_iccid": device_info_param.get("iccid", ""),
                "device_imei": device_info["imei"],
                "device_type": device_info["device_type"],
                "group_list": group_list,
                "di_list": device_info_config.get("terminal_settings", {}).get("di_list", {}),
                "do_list": device_info_config.get("terminal_settings", {}).get("do_list", {}),
                "do_timer_list": device_info_config.get("terminal_settings", {}).get(
                    "do_timer_list", {}
                ),
                "ai_list": device_info_config.get("terminal_settings", {}).get("ai_list", {}),
            }
        )
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


def num_to_str(obj):
    if isinstance(obj, dict):
        return {key: num_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [num_to_str(item) for item in obj]
    elif isinstance(obj, (int, float, Decimal)):
        return str(obj)
    else:
        return obj
