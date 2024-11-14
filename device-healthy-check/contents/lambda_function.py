import os
import boto3
import db
import ddb
import ssm
import time
import traceback
import json
from validate import validate
from device_healthy import device_healthy
from di_healthy import di_healthy
from mail_notice import mailNotice
from automation_trigger import automationTrigger
from datetime import datetime
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


def lambda_handler(event, context):
    logger.debug(f"lambda_handler開始 event={event}")

    try:
        # DynamoDB操作オブジェクト生成
        try:
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            state_table = dynamodb.Table(ssm.table_names["STATE_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
            notification_hist_table = dynamodb.Table(ssm.table_names["NOTIFICATION_HIST_TABLE"])
            hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
        except KeyError as e:
            logger.error("KeyError")
            return -1

        for record in event['Records']:
            payload = json.loads(record["body"])

            # 現在日時を取得
            now = datetime.now()
            now_datetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)

            # パラメータ取得
            event_trigger = payload.get("event_trigger")
            device_id = payload.get("device_id")
            di_no = payload.get("di_no")
            contract_id = payload.get("contract_id")
            event_type = payload.get("event_type")
            event_datetime = payload.get("event_datetime")
            logger.debug(f"event_trigger={event_trigger}, device_id={device_id}, contract_id={contract_id}, event_datetime={event_datetime}")

            # パラメータチェック
            validate_result = validate(event_trigger, device_id, di_no, contract_id, event_type, event_datetime, device_table)
            if validate_result != 0:
                return -1

            # 対象デバイス取得
            if event_trigger == "lambda-device-healthy-check-trigger":
                healthy_datetime = now_datetime

                # 契約IDをキーにデバイス情報取得
                device_list = ddb.get_device_info_by_contract_id(contract_id, contract_table, device_table)
                logger.debug(f"device_list={device_list}")

            else:
                logger.error("event_trigger不正")
                return -1

            for device_info in device_list:

                # 履歴一覧リスト作成
                hist_list_items = []

                # 現状態取得
                device_id = device_info["device_id"]
                device_current_state = ddb.get_device_state(device_id, state_table)
                logger.debug(f"device_current_state={device_current_state}")

                # 現状態判定
                if device_current_state is None:
                    logger.debug(f"現状態未登録 device_id={device_id}")
                    continue

                # グループ情報取得
                group_list = []
                group_list = ddb.get_device_group_list(
                    device_info.get("device_id"), device_relation_table, group_table
                )
                logger.debug(f"group_list={group_list}")

                # グループ名
                group_name_list = []
                for group_info in group_list:
                    group_name_list.append(group_info.get("group_name"))
                group_name = "、".join(group_name_list)
                logger.debug(f"group_name={group_name}")

                # 現状態更新タイプ
                update_digit = 0b0000

                # デバイスヘルシーチェック
                if event_trigger == "lambda-device-healthy-check-trigger":
                    device_current_state, hist_list_items = device_healthy(device_info, now_datetime, device_current_state, hist_list_items, healthy_datetime, group_list)
                    if hist_list_items:
                        update_digit |= 0b0001

                # DIヘルシーチェック
                if event_trigger == "lambda-device-healthy-check-trigger":
                    device_current_state, hist_list_items = di_healthy(device_info, di_no, device_current_state, hist_list_items, now_datetime, healthy_datetime, event_trigger, group_list)
                    if hist_list_items:
                        update_digit |= 0b0010

                if hist_list_items:
                    # 現状態更新
                    ddb.update_current_state(device_id, update_digit, device_current_state, state_table)
                    logger.debug("現状態更新")

                    # メール通知
                    hist_list_items = mailNotice(
                        device_info, group_name, hist_list_items, now_datetime, user_table, account_table, notification_hist_table
                    )

                    # 履歴一覧挿入
                    ddb.put_cnt_hist_list(hist_list_items, hist_list_table)

                    # 連動制御処理呼出
                    automationTrigger(hist_list_items)

        logger.debug("lambda_handler正常終了")
        return 0

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        raise Exception('lambda_handler異常終了')
