import json
import os

import boto3
from aws_lambda_powertools import Logger

import auth
import db
import ssm
import validate
import ddb

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user
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
        if user.get("user_type") != "admin" and user.get("user_type") != "sub_admin":
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        contract = db.get_contract_info(user["contract_id"], contract_table)
        notification_list = body["notification_list"]

        # 権限チェック
        for notification in notification_list:
            if notification["device_id"] not in contract["contract_data"]["device_list"]:
                res_body = {"message": "不正なデバイスIDが指定されています。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
            for user_id in notification["notification_target_list"]:
                if user_id not in contract["contract_data"]["user_list"]:
                    res_body = {"message": "不正なユーザーIDが指定されています。"}
                    return {
                        "statusCode": 400,
                        "headers": res_headers,
                        "body": json.dumps(res_body, ensure_ascii=False),
                    }

        # リクエストの通知設定をデバイスIDごとにまとめる
        notificaton_settings_list = {}
        for notification in notification_list:
            device_id = notification["device_id"]
            notificaton_settings = notificaton_settings_list.get(device_id, [])
            notificaton_settings.append(
                {
                    "event_trigger": notification.get("event_trigger", ""),
                    "terminal_no": notification.get("terminal_no", ""),
                    "event_type": notification.get("event_type", ""),
                    "change_detail": notification.get("change_detail", ""),
                    "notification_target_list": notification.get("notification_target_list", []),
                }
            )
            notificaton_settings_list[device_id] = notificaton_settings

        # デバイス管理テーブルの通知設定更新
        ddb.update_device_notification_settings(notificaton_settings_list, device_table)

        # 通知設定を取得しなおして返却
        device_list = contract.get("contract_data", {}).get("device_list", {})
        notification_list = []
        for device_id in device_list:
            device = db.get_device_info(device_id, device_table)
            notification_settings = (
                device.get("device_data", {}).get("config", {}).get("notification_settings", {})
            )
            logger.debug(notification_settings)
            for notification_setting in notification_settings:
                notification_list.append(
                    {
                        "device_id": device_id,
                        "device_name": device.get("device_data", {})
                        .get("config", {})
                        .get("device_name", ""),
                        "device_imei": device.get("imei", ""),
                        "event_trigger": notification_setting.get("event_trigger", ""),
                        "terminal_no": notification_setting.get("terminal_no", ""),
                        "change_detail": notification_setting.get("change_detail", ""),
                        "notification_target_list": notification_setting.get(
                            "notification_target_list", []
                        ),
                    }
                )

        res_body = {
            "message": "",
            "notification_list": notification_list,
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
