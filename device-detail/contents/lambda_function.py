import json
import boto3
import validate
import generate_detail
import ddb
import os
import re
import logging
from botocore.exceptions import ClientError
from decimal import Decimal
import traceback

# layer
import db
import ssm
import convert

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

parameter = None
logger = logging.getLogger()


def lambda_handler(event, context):
    try:
        res_headers = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}
        # コールドスタートの場合パラメータストアから値を取得してグローバル変数にキャッシュ
        global parameter
        if not parameter:
            print("try ssm get parameter")
            response = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(response)
            print("tried ssm get parameter")
        else:
            print("passed ssm get parameter")
        # DynamoDB操作オブジェクト生成
        try:
            tables = {
                "user_table": dynamodb.Table(parameter.get("USER_TABLE")),
                "device_table": dynamodb.Table(parameter.get("DEVICE_TABLE")),
                "group_table": dynamodb.Table(parameter.get("GROUP_TABLE")),
                "device_state_table": dynamodb.Table(parameter.get("STATE_TABLE")),
                "account_table": dynamodb.Table(parameter.get("ACCOUNT_TABLE")),
                "contract_table": dynamodb.Table(parameter.get("CONTRACT_TABLE")),
                "device_relation_table": dynamodb.Table(parameter.get("DEVICE_RELATION_TABLE")),
            }
        except KeyError as e:
            parameter = None
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        ##################
        # 1 入力情報チェック
        ##################
        validate_result = validate.validate(event, tables)
        if validate_result["code"] != "0000":
            return {
                "statusCode": 200,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }
        device_id = validate_result["device_id"]
        print(f"デバイスID:{device_id}")

        ##################
        # 4 デバイス情報取得
        ##################
        try:
            # 4.1 デバイス設定取得
            device_info = ddb.get_device_info(device_id, tables["device_table"]).get("Items", {})
            if len(device_info) == 0:
                res_body = {"code": "9999", "message": "デバイス情報が存在しません。"}
                return {
                    "statusCode": 500,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
            elif len(device_info) >= 2:
                res_body = {
                    "code": "9999",
                    "message": "デバイスIDに「契約状態:初期受信待ち」「契約状態:使用可能」の機器が複数紐づいています",
                }
                return {
                    "statusCode": 500,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
            # 4.2 デバイス現状態取得
            device_state = db.get_device_state(device_id, tables["device_state_table"]).get(
                "Item", {}
            )
            # 4.3 グループ情報取得
            group_info_list = []
            device_group_relation = db.get_device_relation(
                f"d-{device_id}",
                tables["device_relation_table"],
                sk_prefix="g-",
                gsi_name="key2_index",
            )
            print(device_group_relation)
            for item1 in device_group_relation:
                item1 = item1["key1"]
                group_info = db.get_group_info(re.sub("^g-", "", item1), tables["group_table"])
                if "Item" in group_info:
                    group_info_list.append(group_info["Item"])
            # 4.4 デバイス詳細情報生成
            res_body = num_to_str(
                generate_detail.get_device_detail(device_info[0], device_state, group_info_list)
            )
        except ClientError as e:
            print(e)
            body = {"code": "9999", "message": "デバイス詳細の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        print(f"レスポンスボディ:{res_body}")
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
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
