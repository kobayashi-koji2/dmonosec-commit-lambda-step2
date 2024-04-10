import os
import boto3
import uuid
import db
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from datetime import datetime
from dateutil import relativedelta

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

HIST_LIST_TTL = int(os.environ["HIST_LIST_TTL"])

logger = Logger()


def di_healthy(device_info, di_no, device_current_state, hist_list_items, now_datetime, healthy_datetime, event_trigger, group_list):
    logger.debug(f"di_healthy開始 device_info={device_info}")

    # 接点入力端子数分ループ
    for di_info in device_info.get("device_data", {}).get("config", {}).get("terminal_settings", {}).get("di_list", []):
        if event_trigger in ["lambda-receivedata-2", "lambda_device_settings"] and di_info.get("di_no") != di_no:
            continue

        # DIヘルシーチェック情報チェック
        di_no = di_info.get("di_no")
        di_healthy_period = di_info.get("di_healthy_period", 0)
        di_healthy_type = di_info.get("di_healthy_type")
        logger.debug(f"di_healthy_period={di_healthy_period}, di_healthy_type={di_healthy_type}")
        if di_healthy_period == 0 or di_healthy_period is None:
            logger.debug(f"DIヘルシー未設定")
            continue

        # 接点入力_未変化検出_期間をUNIXミリ秒に変換
        if di_healthy_type == "hour":
            di_healthy_period_time = di_healthy_period * 60 * 60 * 1000
        elif di_healthy_type == "day":
            di_healthy_period_time = di_healthy_period * 24 * 60 * 60 * 1000
        else:
            logger.debug(f"接点入力_未変化検出_期間不正値")
            continue

        # DIヘルシーチェック
        di_last_change_datetime = f"di{di_no}_last_change_datetime"
        logger.debug(f"di_last_change_datetime={di_last_change_datetime}")
        if di_last_change_datetime in device_current_state:
            last_recv_datetime = device_current_state.get(di_last_change_datetime)
            elapsed_time = now_datetime - last_recv_datetime
            logger.debug(f"now_datetime={now_datetime}, last_recv_datetime={last_recv_datetime}")
            logger.debug(f"elapsed_time={elapsed_time}, di_healthy_period_time={di_healthy_period_time}")
            if elapsed_time >= di_healthy_period_time:
                di_healthy_state = 1
                # 発生日時 = 最終受信日時 + アラート期間
                healthy_datetime = int(last_recv_datetime + di_healthy_period_time)
            else:
                di_healthy_state = 0
        else:
            logger.debug(f"接点入力{di_no}_最終変化検知日時 device_id={device_info.get("device_id")}")
            continue

        # 現状態初期化
        current_di_healthy_state = f"di{di_no}_healthy_state"
        if current_di_healthy_state not in device_current_state:
            device_current_state[current_di_healthy_state] = 0

        # 現状態比較
        if device_current_state[current_di_healthy_state] != di_healthy_state:
            device_current_state[current_di_healthy_state] = di_healthy_state
            logger.debug(f"DIヘルシー状態変化 di_healthy_state={di_healthy_state}")
        else:
            logger.debug(f"DIヘルシー状態未変化 device_id={device_info.get("device_id")}")
            continue

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
                "group_list": group_list,
                "event_type": "di_unhealthy",
                "terminal_no": di_info.get("di_no"),
                "terminal_name": di_info.get("di_name", f"接点入力{di_info.get("di_no")}"),
                "di_healthy_type": di_healthy_type,
                "di_healthy_period": di_healthy_period,
                "occurrence_flag": di_healthy_state,
            },
        }
        if di_healthy_state == 0:
            hist_list_item["recv_datetime"] = healthy_datetime
        hist_list_items.append(hist_list_item)

    logger.debug("di_healthy正常終了")
    return device_current_state, hist_list_items
