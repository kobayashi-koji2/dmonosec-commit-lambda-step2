import json
import os

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

import auth
import db
import ssm
import validate
import ddb
import convert

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user()
@validate.validate_parameter
def lambda_handler(event, context, user, body):
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

        if user.get("user_type") != "admin" and user.get("user_type") != "sub_admin":
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        contract = db.get_contract_info(user["contract_id"], contract_table)
        notification_list = body["notification_list"]
        
        # 権限チェック
        if device_id not in contract["contract_data"]["device_list"]:
            res_body = {"message": "不正なデバイスIDが指定されています。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(event, body, device_table)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        notification_target_list = body["notification_target_list"]
        for user_id in notification_target_list:
            if user_id not in contract["contract_data"]["user_list"]:
                res_body = {"message": "不正なユーザーIDが指定されています。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }

        # メール通知設定生成
        notificaton_settings = []
        for notification in notification_list:
            # 重複チェック
            for notification_setting in notificaton_settings:
                if (
                    notification_setting.get("event_trigger") == notification.get("event_trigger")
                    and notification_setting.get("terminal_no") == notification.get("terminal_no")
                    and notification_setting.get("event_type") == notification.get("event_type")
                    and notification_setting.get("change_detail") == notification.get("change_detail")
                    and notification_setting.get("custom_event_id") == notification.get("custom_event_id")
                ):
                    res_body = {"message": "同じ設定が重複しています。"}
                    return {
                        "statusCode": 400,
                        "headers": res_headers,
                        "body": json.dumps(res_body, ensure_ascii=False),
                    }

            notificaton_settings.append(
                {
                    "event_trigger": notification.get("event_trigger", ""),
                    "terminal_no": notification.get("terminal_no", ""),
                    "event_type": notification.get("event_type", ""),
                    "change_detail": notification.get("change_detail", ""),
                    "custom_event_id": notification.get("custom_event_id", ""),
                }
            )

        # デバイス管理テーブルの通知設定更新
        ddb.update_device_notification_settings(device_id, notificaton_settings, notification_target_list, device_table)

        # デバイス情報取得
        logger.debug(device_id)
        device = db.get_device_info_other_than_unavailable(device_id, device_table)
        logger.info({"device": device})

        # 通知設定取得
        notification_settings = device.get("device_data", {}).get("config", {}).get("notification_settings", {})
        notification_target_list = device.get("device_data", []).get("config", []).get("notification_target_list", [])
        logger.debug(notification_settings)
        notification_list = []
        for notification_setting in notification_settings:
            notification_list.append(
                {
                    "event_trigger": notification_setting.get("event_trigger", ""),
                    "terminal_no": notification_setting.get("terminal_no", ""),
                    "event_type": notification_setting.get("event_type", ""),
                    "change_detail": notification_setting.get("change_detail", "")
                }
            )

        logger.debug(notification_list)
        logger.debug(notification_target_list)

        res_body = {
            "message": "",
            "notification_list": notification_list,
            "notification_target_list": notification_target_list,
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
