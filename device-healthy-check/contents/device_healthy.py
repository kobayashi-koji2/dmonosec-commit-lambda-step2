import os
import boto3
import uuid
import time
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from datetime import datetime
from dateutil import relativedelta

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

HIST_LIST_TTL = int(os.environ["HIST_LIST_TTL"])

logger = Logger()


def device_healthy(device_info, now_datetime, device_current_state, hist_list_items, healthy_datetime, group_list):
    logger.debug(f"device_healthy開始 device_info={device_info}, ")

    event_datetime = now_datetime

    # デバイスヘルシーチェック情報チェック
    device_healthy_period = device_info.get("device_data", {}).get("config", {}).get("device_healthy_period", 0)
    logger.debug(f"device_healthy_period={device_healthy_period}")
    if device_healthy_period == 0 or device_healthy_period is None:
        logger.debug(f"デバイスヘルシー未設定")
        return device_current_state, hist_list_items

    last_recv_datetime = None
    if device_info.get("device_type") == "UnaTag":
        last_recv_datetime = device_current_state.get("unatag_last_recv_datetime")
    else:
        last_recv_datetime = device_current_state.get("device_abnormality_last_update_datetime")

    # デバイスヘルシーチェック
    if last_recv_datetime:
        elapsed_time = now_datetime - last_recv_datetime
        device_healthy_period_time = device_healthy_period * 24 * 60 * 60 * 1000
        logger.debug(f"now_datetime={now_datetime}, last_recv_datetime={last_recv_datetime}")
        logger.debug(f"elapsed_time={elapsed_time}, device_healthy_period_time={device_healthy_period_time}")
        if elapsed_time >= device_healthy_period_time:
            device_healthy_state = 1
            now = datetime.now()
            event_datetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)
        else:
            device_healthy_state = 0
    else:
        logger.debug(f"ヘルスチェック該当日時が未設定 device_id={device_info.get("device_id")}")
        return device_current_state, hist_list_items

    # 現状態初期化
    if "device_healthy_state" not in device_current_state:
        device_current_state["device_healthy_state"] = 0

    # 現状態比較
    if device_current_state["device_healthy_state"] != device_healthy_state:
        device_current_state["device_healthy_state"] = device_healthy_state
        logger.debug(f"デバイスヘルシー状態変化 {device_healthy_state}")
    else:
        logger.debug(f"デバイスヘルシー状態未変化 device_id={device_info.get("device_id")}")
        return device_current_state, hist_list_items

    # 履歴一覧データ作成
    expire_datetime = int((datetime.fromtimestamp(event_datetime / 1000) + relativedelta.relativedelta(years=HIST_LIST_TTL)).timestamp())
    hist_list_item = {
        "device_id": device_info.get("device_id"),
        "hist_id": str(uuid.uuid4()),
        "event_datetime": event_datetime,
        "expire_datetime": expire_datetime,
        "hist_data": {
            "device_name": device_info.get("device_data", {}).get("config", {}).get("device_name"),
            "imei": device_info.get("imei"),
            "group_list": group_list,
            "event_type": "device_unhealthy",
            "device_healthy_period": device_healthy_period,
            "occurrence_flag": device_healthy_state,
        }
    }
    if device_healthy_state == 0:
        hist_list_item["recv_datetime"] = healthy_datetime
    hist_list_items.append(hist_list_item)

    logger.debug("device_healthy正常終了")
    return device_current_state, hist_list_items
