import json
import os

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

import auth
import db
import ssm
import ddb

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user()
def lambda_handler(event, context, user):
    res_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }
    logger.info({"user": user})

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

    try:
        pathParam = event.get("pathParameters", {})
        if not pathParam or "device_id" not in pathParam:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "パラメータが不正です。"}, ensure_ascii=False),
            }
        device_id = pathParam["device_id"]
        if not device_id:
            return {"message": "リクエストパラメータが不正です。"}

        if user.get("user_type") != "admin" and user.get("user_type") != "sub_admin":
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        contract = db.get_contract_info(user["contract_id"], contract_table)

        # 権限チェック
        if device_id not in contract["contract_data"]["device_list"]:
            res_body = {"message": "不正なデバイスIDが指定されています。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        # 契約状態チェック
        device = db.get_device_info_other_than_unavailable(device_id, device_table)
        if device is None:
            res_body = {"message": "デバイス情報が存在しません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        # デバイス管理テーブルの通知設定削除
        ddb.delete_device_notification_settings(device_id, device_table)

        return {
            "statusCode": 204,
            "headers": res_headers,
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
