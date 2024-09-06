import json
import os
import boto3
import ddb
from datetime import datetime
import time
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import ssm
import convert

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
DEVICE_ANNOUNCEMENT = {
    "shipped": "デバイス（【{device_code}】{IMEI}（IMEI））が出荷されました。",
    "regist_balance_days": "デバイス（【{device_code}】{IMEI}（IMEI））の利用登録を実施してください（残り{balance_day}日）。",
    "auto_regist_complete": "デバイス（【{device_code}】{IMEI}（IMEI））の利用登録されないまま７日間経過したので、利用を開始しました。",
}

logger = Logger()

def convert_to_full_width(number):
    normal_numbers = "0123456789"
    full_width_numbers = "０１２３４５６７８９"
    trans_table = str.maketrans(normal_numbers, full_width_numbers)
    return str(number).translate(trans_table)

@auth.verify_login_user()
def lambda_handler(event, context, user):
    res_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }
    logger.info({"user": user})

    # DynamoDB操作オブジェクト生成
    try:
        announcement_table = dynamodb.Table(ssm.table_names["ANNOUNCEMENT_TABLE"])
        device_announcement_table = dynamodb.Table(ssm.table_names["DEVICE_ANNOUNCEMENT_TABLE"])
        user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
    except KeyError as e:
        body = {"message": e}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }

    try:
        # ユーザー情報取得
        user_id = user.get("user_id")
        user_type = user.get("user_type")
        contract_id = user.get("contract_id")

        # 現在日時取得
        now = datetime.now()
        now_unixtime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)
        logger.debug(f"now_unixtime: {now_unixtime}")

        # システムメンテナンス情報取得
        announcement_type = "system_maintenance"
        system_maintenance_list = ddb.get_announcement_list(announcement_table, announcement_type, now_unixtime)
        logger.debug(f"system_maintenance_list: {system_maintenance_list}")

        # 重要なお知らせ取得
        announcement_type = "important_announcement"
        important_announcement_list = ddb.get_announcement_list(announcement_table, announcement_type, now_unixtime)
        logger.debug(f"important_announcement_list: {important_announcement_list}")

        # デバイスお知らせ情報取得
        # ユーザー権限が管理者、副管理者のみ
        if user_type in ["admin", "sub_admin"]:
            device_announcement_list = ddb.get_device_announcement_list(device_announcement_table, contract_id)
            logger.debug(f"device_announcement_list: {device_announcement_list}")
        else:
            device_announcement_list = []

        # デバイス関連メッセージ生成
        device_related_list = []
        for device_announcement in device_announcement_list:
            device_announcement_type = device_announcement.get("device_announcement_type")
            if device_announcement_type in DEVICE_ANNOUNCEMENT:
                if device_announcement_type == "regist_balance_days":
                    dev_reg_datetime = int(device_announcement.get("dev_reg_datetime", 0))
                    balance_day = convert_to_full_width(7 - abs(datetime.fromtimestamp(now_unixtime / 1000) - datetime.fromtimestamp(dev_reg_datetime / 1000)).days)
                    device_related_list.append(
                        DEVICE_ANNOUNCEMENT[device_announcement_type].format(
                            IMEI=device_announcement.get("imei", ""),
                            device_code=device_announcement.get("device_code", ""),
                            balance_day=balance_day,
                        )
                    )
                else:
                    device_related_list.append(
                        DEVICE_ANNOUNCEMENT[device_announcement_type].format(
                            IMEI=device_announcement.get("imei", ""),
                            device_code=device_announcement.get("device_code", ""),
                        )
                    )
            else:
                continue
        logger.debug(f"device_related_list: {device_related_list}")

        # お知らせ画面最終表示日時更新
        ddb.update_user_announcement_last_display_datetime(user_table, user_id, now_unixtime)

        res_body = {
            "message": "",
            "system_maintenance_list": system_maintenance_list,
            "important_announcement_list": important_announcement_list,
            "device_related_list": device_related_list,
        }
        logger.debug(f"res_body: {res_body}")
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
