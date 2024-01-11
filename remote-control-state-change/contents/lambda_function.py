import json
import os
import time
import traceback

from aws_lambda_powertools import Logger
import boto3

import ddb
import validate

# layer
import db
import ssm


parameter = None
logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

# レスポンスヘッダー
response_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}


def lambda_handler(event, context):
    try:
        # コールドスタートの場合パラメータストアから値を取得してグローバル変数にキャッシュ
        global parameter
        if not parameter:
            logger.info("try ssm get parameter")
            response = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(response)
            logger.info("tried ssm get parameter")
        else:
            logger.info("passed ssm get parameter")
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(parameter["USER_TABLE"])
            contract_table = dynamodb.Table(parameter["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(parameter["DEVICE_RELATION_TABLE"])
            remote_control_table = dynamodb.Table(parameter["REMOTE_CONTROL_TABLE"])
            cnt_hist_table = dynamodb.Table(parameter["CNT_HIST_TABLE"])
        except KeyError as e:
            parameter = None
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": response_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # 入力情報のバリデーションチェック
        validate_result = validate.validate(event, user_table)
        if validate_result["code"] != "0000":
            logger.info("Error in validation check of input information.")
            return {
                "statusCode": 200,
                "headers": response_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        # トークンからユーザー情報取得
        user_info = validate_result["user_info"]
        logger.info(user_info)

        # 権限が参照者の場合はエラー
        if user_info["user_type"] == "referrer":
            return {
                "statusCode": 200,
                "headers": response_headers,
                "body": json.dumps({"code": "9999", "message": "権限がありません。"}, ensure_ascii=False),
            }

        # 通信制御情報取得
        user_id = user_info["user_id"]
        device_req_no = event["pathParameters"]["device_req_no"]
        logger.info(f"device_req_no: {device_req_no}")
        remote_control = db.get_remote_control(device_req_no, remote_control_table)

        logger.info(f"remote_control: {remote_control}")

        if remote_control is None:
            return {
                "statusCode": 200,
                "headers": response_headers,
                "body": json.dumps(
                    {"code": "9999", "message": "端末要求が存在しません。"}, ensure_ascii=False
                ),
            }

        device_id = remote_control["device_id"]

        # デバイス操作権限チェック（共通）
        contract = db.get_contract_info(user_info["contract_id"], contract_table)["Item"]
        logger.info(f"contract: {contract}")
        device_id_list_by_contract = contract["contract_data"]["device_list"]

        if device_id not in device_id_list_by_contract:
            return {
                "statusCode": 200,
                "headers": response_headers,
                "body": json.dumps(
                    {"code": "9999", "message": "端末の操作権限がありません。"}, ensure_ascii=False
                ),
            }

        # デバイス操作権限チェック（管理者, 副管理者でない場合）
        if user_info["user_type"] not in ["admin", "sub_admin"]:
            allowed_device_id_list = get_device_id_list_by_user_id(user_id, device_relation_table)
            logger.info(f"allowed_device_id_list: {allowed_device_id_list}")

            if device_id not in allowed_device_id_list:
                return {
                    "statusCode": 200,
                    "headers": response_headers,
                    "body": json.dumps(
                        {"code": "9999", "message": "端末の操作権限がありません。"}, ensure_ascii=False
                    ),
                }

        # 状態変化通知確認
        recv_datetime = remote_control["recv_datetime"]
        limit_datetime = recv_datetime + 20000  # 20秒
        control_result = "1"

        logger.info(int(time.time() * 1000))
        while int(time.time() * 1000) <= limit_datetime:
            cnt_hist_list = ddb.get_cnt_hist_list_by_sim_id(
                remote_control["iccid"],
                cnt_hist_table,
                recv_datetime,
                limit_datetime,
            )
            logger.info(f"cnt_hist_list: {cnt_hist_list}")
            for cnt_hist in cnt_hist_list:
                if remote_control["link_di_no"] == cnt_hist["di_trigger"]:
                    control_result = "0"
                    break

            if control_result == "0":
                break

            time.sleep(1)

        return {
            "statusCode": 200,
            "headers": response_headers,
            "body": json.dumps(
                {
                    "code": "0000",
                    "message": "",
                    "device_req_no": device_req_no,
                    "control_result": control_result,
                },
                ensure_ascii=False,
            ),
        }

    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        return {
            "statusCode": 500,
            "headers": response_headers,
            "body": json.dumps(
                {"code": "9999", "message": "予期しないエラーが発生しました。"}, ensure_ascii=False
            ),
        }


def get_device_id_list_by_user_id(user_id, device_relation_table):
    device_id_list = []
    device_relation_list = db.get_device_relation(f"u-{user_id}", device_relation_table)

    for device_relation in device_relation_list:
        if device_relation["key2"].startswith("d-"):
            device_id_list.append(device_relation["key2"][2:])

        elif device_relation["key2"].startswith("g-"):
            device_relation_list_by_group_id = db.get_device_relation(
                device_relation["key2"], device_relation_table, sk_prefix="d-"
            )
            device_id_list += [
                relation["key2"][2:] for relation in device_relation_list_by_group_id
            ]

    return device_id_list
