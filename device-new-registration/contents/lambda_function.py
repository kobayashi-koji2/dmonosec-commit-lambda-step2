import json
import os
import time
import traceback
import uuid

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import convert
import db
import ddb
import ssm
import validate

patch_all()

logger = Logger()

# 環境変数
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]

# レスポンスヘッダー
res_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}

# AWSリソース定義
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_DEFAULT_REGION,
    endpoint_url=os.environ.get("endpoint_url"),
)


@auth.verify_login_user()
@validate.validate_request_body
def lambda_handler(event, context, user_info, request_body):
    try:
        ### 0. DynamoDBの操作オブジェクト生成
        pre_register_table = dynamodb.Table(ssm.table_names["PRE_REGISTER_DEVICE_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        device_announcement_table = dynamodb.Table(ssm.table_names["DEVICE_ANNOUNCEMENT_TABLE"])

        ### 1. 入力情報チェック
        # ユーザー権限確認
        operation_auth = __operation_auth_check(user_info)
        if not operation_auth:
            res_body = {"message": "ユーザに操作権限がありません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        device_imei = request_body["device_imei"]
        contract_id = user_info["contract_id"]

        ### 2. デバイス情報登録
        transact_items = list()
        pre_device_info = ddb.get_pre_reg_device_info_by_imei(device_imei, contract_id, pre_register_table)
        if not pre_device_info:
            res_body = {"message": "登録前デバイス情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.debug(f"pre_device_info: {pre_device_info}")

        device_id = str(uuid.uuid4())
        contract_id = pre_device_info["contract_id"]

        # デバイス種別の判定
        if pre_device_info["device_code"] == "MS-C0100":
            device_type = "PJ1"
        elif pre_device_info["device_code"] == "MS-C0110":
            device_type = "PJ2"
        elif pre_device_info["device_code"] == "MS-C0120":
            device_type = "PJ3"
        else:
            res_body = {"message": "機器コードの値が不正です。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        device_data = {
            "param": {
                "iccid": pre_device_info["iccid"],
                "imsi": pre_device_info["imsi"],
                "device_code": pre_device_info["device_code"],
                "contract_id": contract_id,
                "dev_reg_datetime": int(pre_device_info["dev_reg_datetime"]),
                "coverage_url": pre_device_info["coverage_url"],
                "use_type": 0,
                "dev_user_reg_datetime": int(time.time() * 1000),
                "service": "monosc",
            },
            "config": __generate_device_data_config(device_type),
        }
        put_item = {
            "device_id": device_id,
            "identification_id": device_imei,
            "contract_state": 0,
            "device_data": device_data,
            "device_type": device_type,
        }
        put_item_fmt = convert.dict_dynamo_format(put_item)
        put_device = {
            "Put": {
                "TableName": ssm.table_names["DEVICE_TABLE"],
                "Item": put_item_fmt,
            }
        }
        transact_items.append(put_device)
        logger.debug(f"put_device_info: {put_device}")

        ### 3. 契約情報更新
        contract_info = db.get_contract_info(contract_id, contract_table)
        update_item = contract_info["contract_data"]["device_list"]
        update_item.append(device_id)

        update_item_fmt = convert.to_dynamo_format(update_item)
        update_contract = {
            "Update": {
                "TableName": ssm.table_names["CONTRACT_TABLE"],
                "Key": {"contract_id": {"S": contract_id}},
                "UpdateExpression": "SET #cd.#dl = :s",
                "ExpressionAttributeNames": {"#cd": "contract_data", "#dl": "device_list"},
                "ExpressionAttributeValues": {":s": update_item_fmt},
            }
        }
        transact_items.append(update_contract)
        logger.debug(f"update_contract_info: {update_contract}")

        ### 4. デバイス関連情報追加
        put_item = {
            "imei": device_imei,
            "contract_id": contract_id,
            "device_id": device_id,
        }
        put_item_fmt = convert.dict_dynamo_format(put_item)
        put_imei = {
            "Put": {
                "TableName": ssm.table_names["IMEI_TABLE"],
                "Item": put_item_fmt,
            }
        }
        transact_items.append(put_imei)
        logger.debug(f"put_imei: {put_imei}")

        put_item = {
            "iccid": pre_device_info["iccid"],
            "contract_id": contract_id,
            "device_id": device_id,
        }
        put_item_fmt = convert.dict_dynamo_format(put_item)
        put_iccid = {
            "Put": {
                "TableName": ssm.table_names["ICCID_TABLE"],
                "Item": put_item_fmt,
            }
        }
        transact_items.append(put_iccid)
        logger.debug(f"put_iccid: {put_iccid}")

        ### 5. 登録前デバイス削除
        delete_pre_register = {
            "Delete": {
                "TableName": ssm.table_names["PRE_REGISTER_DEVICE_TABLE"],
                "Key": {"identification_id": {"S": device_imei}},
            }
        }
        transact_items.append(delete_pre_register)
        logger.debug(f"delete_pre_register: {delete_pre_register}")

        ### 6. デバイス関連お知らせ情報削除
        device_announcements = ddb.get_device_announcement_list(
            device_announcement_table, device_imei
        )
        if device_announcements:
            delete_device_announcements = {
                "Delete": {
                    "TableName": ssm.table_names["DEVICE_ANNOUNCEMENT_TABLE"],
                    "Key": {
                        "device_announcement_id": {
                            "S": device_announcements.get("device_announcement_id")
                        }
                    },
                }
            }
            transact_items.append(delete_device_announcements)
            logger.debug(f"delete_device_announcements: {delete_device_announcements}")

        # 各データを登録・更新・削除
        if not db.execute_transact_write_item(transact_items):
            res_body = {"message": "DynamoDBへの更新処理に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 6. メッセージ応答
        res_body = {"message": ""}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }


# 操作権限チェック
def __operation_auth_check(user_info):
    user_type = user_info["user_type"]
    logger.debug(f"ユーザー権限: {user_type}")
    if user_type == "admin" or user_type == "sub_admin":
        return True
    return False


def __generate_device_data_config(device_type):
    di_list, do_list = list(), list()
    # 現状はdevice_typeは PJ2 のみ来る想定
    di_count, do_count = 0, 0
    if device_type == "PJ1":
        di_count, do_count = 1, 0
    elif device_type == "PJ2":
        di_count, do_count = 8, 2
    # elif device_type == "PJ3":
    #     di_count, do_count = 8, 2

    for di_no in range(1, di_count + 1):
        di_item = {
            "di_no": di_no,
            "di_name": "接点入力" + str(di_no),
            "di_on_name": "Close",
            "di_on_icon": "on",
            "di_off_name": "Open",
            "di_off_icon": "off",
        }
        di_list.append(di_item)

    for do_no in range(1, do_count + 1):
        do_item = {
            "do_no": do_no,
            "do_name": "接点出力" + str(do_no),
            "do_control": None,
            "do_specified_time": None,
            "do_di_return": 0,
            "do_timer_list": [],
        }
        do_list.append(do_item)

    result = {
        "device_name": None,
        "device_healthy_period": 3,
        "terminal_settings": {"do_list": do_list, "di_list": di_list},
    }

    return result
