import os
import boto3
import db
import ddb
import ssm
import time
import traceback
import validate
import uuid
from mail_notice import mailNotice
from datetime import datetime
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


def hist_list_edit(device_info, device_healthy_state, timestamp, notification_hist_id, group_name):

    # 共通部
    hist_list_data = {
        "device_id": device_info.get("device_id"),
        "hist_id": str(uuid.uuid4()),
        "event_datetime": timestamp,
        "hist_data": {
            "device_name": device_info.get("device_data", {}).get("config", {}).get("device_name"),
            "group_list": group_name,
            "imei": device_info.get("imei"),
            "event_type": "device_unhealthy",
            "occurrence_flag": device_healthy_state,
        },
    }
    if notification_hist_id is not None:
        hist_list_data["hist_data"]["notification_hist_id"] = notification_hist_id
    return hist_list_data


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

        # 現在日時を取得
        now = datetime.now()
        now_datetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)

        # パラメータ取得
        event_trigger = event.get("event_trigger")
        device_id = event.get("device_id")
        contract_id = event.get("contract_id")
        event_datetime = event.get("event_datetime")
        logger.debug(f"event_trigger={event_trigger}, device_id={device_id}, contract_id={contract_id}, event_datetime={event_datetime}")

        # パラメータチェック
        validate_result = validate.validate(event_trigger, device_id, contract_id, event_datetime)
        if validate_result != 0:
            return -1

        # 対象デバイス取得
        device_list = []
        if event_trigger == "lambda-receivedata-2":
            healthy_datetime = event_datetime

            # デバイスIDをキーにデバイス情報取得
            device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
            if device_info is not None:
                device_list.append(device_info)

        elif event_trigger == "lambda-device-healthy-check-trigger":
            healthy_datetime = now_datetime

            # 契約IDをキーにデバイス情報取得
            device_list = ddb.get_device_info_by_contract_id(contract_id, contract_table, device_table)
            logger.debug(f"device_list={device_list}")

        else:
            logger.error("event_trigger不正")
            return -1

        for device_info in device_list:

            # デバイスヘルシーチェック情報チェック
            device_healthy_period = device_info.get("device_data", {}).get("config", {}).get("device_healthy_period", 0)
            logger.debug(f"device_healthy_period={device_healthy_period}")
            if device_healthy_period == 0:
                logger.debug(f"デバイスヘルシー未設定")
                continue

            # 現状態取得
            device_id = device_info["device_id"]
            device_current_state = ddb.get_device_state(device_id, state_table)
            logger.debug(f"device_current_state={device_current_state}")

            # デバイスヘルシーチェック
            if "device_abnormality_last_update_datetime" in device_current_state:
                last_recv_datetime = device_current_state.get("device_abnormality_last_update_datetime")
                elapsed_time = now_datetime - last_recv_datetime
                device_healthy_period_time = device_healthy_period * 24 * 60 * 60 * 1000
                logger.debug(f"now_datetime={now_datetime}, last_recv_datetime={last_recv_datetime}")
                logger.debug(f"elapsed_time={elapsed_time}, device_healthy_period_time={device_healthy_period_time}")
                if elapsed_time >= device_healthy_period_time:
                    device_healthy_state = 1
                else:
                    device_healthy_state = 0
            else:
                logger.debug(f"機器異常_最終更新日時が未設定 device_id={device_id}")
                continue

            # 現状態比較
            if device_current_state.get("device_healthy_state") == device_healthy_state:
                logger.debug(f"ヘルシー状態未変化 device_id={device_id}")
                continue

            # 現状態更新
            ddb.update_current_state(device_id, device_healthy_state, state_table)
            logger.debug("現状態更新")

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

            # メール通知
            notification_hist_id = mailNotice(
                device_info, group_name, device_healthy_state, now_datetime, user_table, account_table, notification_hist_table
            )
            logger.debug(f"notification_hist_id={notification_hist_id}")

            # 履歴一覧挿入
            hist_list_data = hist_list_edit(device_info, device_healthy_state, healthy_datetime, notification_hist_id, group_name)
            logger.debug(f"hist_list_data={hist_list_data}")
            ddb.put_cnt_hist_list(hist_list_data, hist_list_table)

        logger.debug("lambda_handler正常終了")
        return 0

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        return -1
