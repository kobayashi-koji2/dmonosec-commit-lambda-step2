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
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        device_announcement_table = dynamodb.Table(ssm.table_names["DEVICE_ANNOUNCEMENT_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])

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
        device_id = request_body["device_id"]
        
        if "device_imei" in request_body:
            identification_id = request_body["device_imei"]
        elif "device_sigfox_id" in request_body:
            identification_id = request_body["device_sigfox_id"]
        else:
            res_body = {"message": "imeiとsigfox_idのいずれかが未指定です。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
            
        transact_items = list()
        ### 2. デバイス情報更新
        pre_device_info = ddb.get_pre_reg_device_info_by_imei(identification_id, pre_register_table)
        if not pre_device_info:
            res_body = {"message": "登録前デバイス情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.debug(f"pre_device_info: {pre_device_info}")
        
        device_type = ""
        if pre_device_info.get("device_code") == "MS-C0100":
            device_type = "PJ1"
        elif pre_device_info.get("device_code") == "MS-C0110":
            device_type = "PJ2"
        elif pre_device_info.get("device_code") == "MS-C0130":
            device_type = "UnaTag"
        else:
            res_body = {"message": "デバイス種別が不正です。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
        if device_info is None:
            res_body = {"message": "保守交換対象のデバイス情報が存在しません。"}
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.debug(f"device_info: {device_info}")
        if device_info.get("device_type") != device_type:
            res_body = {"message": "デバイス種別が一致しません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        # 新デバイス情報登録
        if device_type in ["PJ1", "PJ2"]:
            put_item = {
                "device_id": device_id,
                "identification_id": identification_id,
                "contract_state": 0,  # 「0:初期受信待ち」で値は固定
                "device_type": device_info["device_type"],
                "contract_id": pre_device_info["contract_id"],
                "device_data": {
                    "param": {
                        "iccid": pre_device_info["iccid"],
                        "imsi": pre_device_info["imsi"],
                        "device_code": pre_device_info["device_code"],
                        "contract_id": pre_device_info["contract_id"],
                        "dev_reg_datetime": int(pre_device_info["dev_reg_datetime"]),
                        "coverage_url": pre_device_info["coverage_url"],
                        "use_type": 1,  # 「1:保守交換」で値は固定
                        "dev_user_reg_datetime": int(time.time() * 1000),
                        "service": "monosc",
                    },
                    "config": device_info["device_data"][
                        "config"
                    ],  # デバイスの「設定情報」は元々の値を引継ぐ
                },
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
        elif device_type == "UnaTag":
            put_item = {
                "device_id": device_id,
                "identification_id": identification_id,
                "contract_state": 0,  # 「0:初期受信待ち」で値は固定
                "device_type": device_info["device_type"],
                "contract_id": pre_device_info["contract_id"],
                "device_data": {
                    "param": {
                        "device_code": pre_device_info["device_code"],
                        "contract_id": pre_device_info["contract_id"],
                        "dev_reg_datetime": int(pre_device_info["dev_reg_datetime"]),
                        "coverage_url": pre_device_info["coverage_url"],
                        "use_type": 1,  # 「1:保守交換」で値は固定
                        "dev_user_reg_datetime": int(time.time() * 1000),
                        "service": "monosc",
                    },
                    "config": device_info["device_data"][
                        "config"
                    ],  # デバイスの「設定情報」は元々の値を引継ぐ
                },
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
        else:
            res_body = {"message": "デバイス種別が不正です。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        # 旧デバイス情報更新（契約状態を「2:利用不可」へ更新）
        bf_device_identification_id = device_info["identification_id"]
        update_contract = {
            "Update": {
                "TableName": ssm.table_names["DEVICE_TABLE"],
                "Key": {"device_id": {"S": device_id}, "identification_id": {"S": bf_device_identification_id}},
                "UpdateExpression": "SET #name1 = :value1",
                "ExpressionAttributeNames": {"#name1": "contract_state"},
                "ExpressionAttributeValues": {":value1": {"N": "2"}},
            }
        }
        transact_items.append(update_contract)
        logger.debug(f"update_bf_device_info: {update_contract}")

        ### 3. デバイス関連情報更新
        # IMEI情報更新
        if device_type in ["PJ1","PJ2"]:
            put_item = {
                "imei": identification_id,
                "contract_id": pre_device_info["contract_id"],
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

        # ICCID情報更新
        put_item = {
            "iccid": pre_device_info.get("iccid"),
            "contract_id": pre_device_info["contract_id"],
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
        
        # SIGFOX_ID情報更新
        if device_type == "UnaTag":
            put_item = {
                "sigfox_id": identification_id,
                "contract_id": pre_device_info["contract_id"],
                "device_id": device_id,
            }
            put_item_fmt = convert.dict_dynamo_format(put_item)
            put_sigfox = {
                "Put": {
                    "TableName": ssm.table_names["SIGFOX_ID_TABLE"],
                    "Item": put_item_fmt,
                }
            }
            transact_items.append(put_sigfox)
            logger.debug(f"put_sigfox: {put_sigfox}")

        ### 4. 登録前デバイス情報削除
        delete_pre_register = {
            "Delete": {
                "TableName": ssm.table_names["PRE_REGISTER_DEVICE_TABLE"],
                "Key": {"identification_id": {"S": identification_id}},
            }
        }
        transact_items.append(delete_pre_register)
        logger.debug(f"delete_pre_register: {delete_pre_register}")
        
        ### 6. デバイス関連テーブル更新
        pre_device_relation_list = db.get_pre_device_relation_group_id_list(identification_id, device_relation_table)
        logger.info(pre_device_relation_list)
        pd_identification_id = "pd-" + identification_id
        for group_id in pre_device_relation_list:
            device_relation_list = db.get_device_relation(
            "g-" + group_id, device_relation_table, sk_prefix="pd-"
            )
            logger.info(device_relation_list)
            for device_relation in device_relation_list:
                if pd_identification_id == device_relation["key2"]:
                    remove_relation = {
                        "Delete": {
                            "TableName": ssm.table_names["DEVICE_RELATION_TABLE"],
                            "Key": {
                                "key1": {"S": device_relation["key1"]},
                                "key2": {"S": device_relation["key2"]},
                            },
                        }
                    }
                    transact_items.append(remove_relation)
            device_relation_item = {
                "key1": "g-" + group_id,
                "key2": "d-" + device_id,
            }
            device_relation_item_fmt = convert.dict_dynamo_format(device_relation_item)
            put_device_relation = {
                "Put": {
                    "TableName": ssm.table_names["DEVICE_RELATION_TABLE"],
                    "Item": device_relation_item_fmt,
                }
            }
            transact_items.append(put_device_relation)
            logger.debug(f"remove_relation: {remove_relation}")

        ### 6. デバイス関連お知らせ情報削除
        device_announcements = ddb.get_device_announcement_list(
            device_announcement_table, identification_id
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

        ### 7. メッセージ応答
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
