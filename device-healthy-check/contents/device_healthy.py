import os
import boto3
import uuid
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from datetime import datetime
from dateutil import relativedelta

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

HIST_LIST_TTL = int(os.environ["HIST_LIST_TTL"])

logger = Logger()


def device_healthy(device_info, now_datetime, device_current_state, hist_list_items, healthy_datetime):
    logger.debug(f"device_healthy開始 device_info={device_info}, ")

    # デバイスヘルシーチェック情報チェック
    device_healthy_period = device_info.get("device_data", {}).get("config", {}).get("device_healthy_period", 0)
    logger.debug(f"device_healthy_period={device_healthy_period}")
    if device_healthy_period == 0 or device_healthy_period is None:
        logger.debug(f"デバイスヘルシー未設定")
        return device_current_state, hist_list_items

    # デバイスヘルシーチェック
    if "device_abnormality_last_update_datetime" in device_current_state:
        last_recv_datetime = device_current_state.get("device_abnormality_last_update_datetime")
        elapsed_time = now_datetime - last_recv_datetime
        device_healthy_period_time = device_healthy_period * 24 * 60 * 60 * 1000
        logger.debug(f"now_datetime={now_datetime}, last_recv_datetime={last_recv_datetime}")
        logger.debug(f"elapsed_time={elapsed_time}, device_healthy_period_time={device_healthy_period_time}")
        if elapsed_time >= device_healthy_period_time:
            device_healthy_state = 1
            # 発生日時 = 最終受信日時 + アラート期間
            healthy_datetime = int(last_recv_datetime + device_healthy_period_time)
        else:
            device_healthy_state = 0
    else:
        logger.debug(f"機器異常_最終更新日時が未設定 device_id={device_info.get("device_id")}")
        return device_current_state, hist_list_items

    # 現状態比較
    if device_current_state.get("device_healthy_state") != device_healthy_state:
        device_current_state["device_healthy_state"] = device_healthy_state
        logger.debug("ヘルシー状態変化 device_healthy_state")
    else:
        logger.debug(f"ヘルシー状態未変化 device_id={device_info.get("device_id")}")
        return device_current_state, hist_list_items

    # 履歴一覧データ作成
    expire_datetime = int((datetime.fromtimestamp(healthy_datetime / 1000) + relativedelta.relativedelta(years=HIST_LIST_TTL)).timestamp())
    hist_list_item = {
        "device_id": device_info.get("device_id"),
        "hist_id": str(uuid.uuid4()),
        "event_datetime": healthy_datetime,
        "expire_datetime": expire_datetime,
        "hist_data": {
            "device_name": device_info.get("device_data", {}).get("config", {}).get("device_name"),
            "imei": device_info.get("imei"),
            "event_type": "device_unhealthy",
            "occurrence_flag": device_healthy_state,
        }
    }
    hist_list_items.append(hist_list_item)

    logger.debug("device_healthy正常終了")
    return device_current_state, hist_list_items
