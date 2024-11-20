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


def customEvent(device_info, device_current_state, hist_list_items, now_unixtime, event_datetime, dt_event, group_list):
    logger.debug(f"customEvent開始 device_info={device_info}")

    # カスタムイベントリスト分ループ
    for custom_event_info in device_info.get("device_data", {}).get("config", {}).get("custom_event_list", []):
        # イベント種別判定
        # 日時指定
        if custom_event_info.get("event_type") == 0:
            custom_event_time = datetime.strptime(custom_event_info.get("time", ""), "%H:%M")
            custom_event_weekday = custom_event_info.get("weekday", "").split(",")
            now_weekday = (dt_event.weekday() + 1) % 7
            if not (
                (custom_event_time.hour == dt_event.hour)
                and (custom_event_time.minute == dt_event.minute)
                and str(now_weekday) in custom_event_weekday
            ):
                logger.debug(f"日時指定アンマッチ")
                continue
            event_type = "custom_datetime"
        # 経過日時
        elif custom_event_info.get("event_type") == 1:
            event_type = "custom_timer"
        else:
            logger.debug(f"カスタムイベント種別不正値")
            continue

        # 接点入力イベントリスト分ループ
        event_hpn_datetime = None
        # 日時指定
        if event_type == "custom_datetime":
            for di_event_info in custom_event_info.get("di_event_list", []):
                di_range = 1 if device_info.get("device_type") == "PJ1" else 8
                for i in range(di_range):
                    terminal_no = i + 1
                    if di_event_info.get("di_no") != terminal_no:
                        continue
                    terminal_key = "di" + str(i + 1) + "_state"
                    current_di_state = device_current_state.get(terminal_key)
                    if di_event_info.get("di_state") in [0,1] and di_event_info.get("di_state") != current_di_state:
                        logger.debug(f"接点入力状態アンマッチ di_state={di_event_info.get("di_state")} current_di_state={current_di_state}")
                        continue
                    event_hpn_datetime = now_unixtime
                    hist_list_items = customEventHist(device_info, hist_list_items, event_datetime, group_list, terminal_no, current_di_state, event_hpn_datetime, custom_event_info, event_type)

        # 経過時間
        elif event_type == "custom_timer":
            custom_timer_event_list = device_current_state.get("custom_timer_event_list", [])
            for custom_timer_event in custom_timer_event_list:
                if custom_timer_event.get("custom_event_id") != custom_event_info.get("custom_event_id"):
                    continue

                for di_event in custom_timer_event.get("di_event_list", []):
                    terminal_no = di_event.get("di_no")
                    if di_event.get("di_custom_event_state") == 1:
                        continue
                    custom_event_period_time = custom_event_info.get("elapsed_time") * 60 * 1000
                    di_last_change_datetime = f"di{terminal_no}_last_change_datetime"
                    logger.debug(f"di_last_change_datetime={di_last_change_datetime}")
                    if di_last_change_datetime in device_current_state:
                        last_change_datetime = device_current_state.get(di_last_change_datetime)
                        elapsed_time = event_datetime - last_change_datetime
                        logger.debug(f"event_datetime={event_datetime}, last_change_datetime={last_change_datetime}")
                        logger.debug(f"elapsed_time={elapsed_time}, custom_event_period_time={custom_event_period_time}")
                        if elapsed_time >= custom_event_period_time:
                            event_hpn_datetime = device_current_state.get(di_last_change_datetime) + custom_event_period_time
                            hist_list_items = customEventHist(device_info, hist_list_items, event_datetime, group_list, terminal_no, current_di_state, event_hpn_datetime, custom_event_info, event_type)
    logger.debug("customEvent正常終了")
    return hist_list_items


def customEventHist(device_info, hist_list_items, event_datetime, group_list, terminal_no, current_di_state, event_hpn_datetime, custom_event_info, event_type):
    now = datetime.now()
    event_datetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)

    # 履歴一覧データ作成
    for di_list in (
        device_info.get("device_data", {})
        .get("config", {})
        .get("terminal_settings", {})
        .get("di_list", [])
    ):
        if int(di_list.get("di_no")) == int(terminal_no):
            terminal_name = di_list.get("di_name", f"接点入力{terminal_no}")
            if current_di_state == 0:
                terminal_state_name = di_list.get("di_on_name", "クローズ")
            else:
                terminal_state_name = di_list.get("di_off_name", "オープン")
            break

    expire_datetime = int((datetime.fromtimestamp(event_datetime / 1000) + relativedelta.relativedelta(years=HIST_LIST_TTL)).timestamp())
    hist_list_item = {
        "device_id": device_info.get("device_id"),
        "hist_id": str(uuid.uuid4()),
        "event_datetime": event_hpn_datetime,
        "expire_datetime": expire_datetime,
        "hist_data": {
            "device_name": device_info.get("device_data", {}).get("config", {}).get("device_name"),
            "imei": device_info.get("imei"),
            "group_list": group_list,
            "event_type": event_type,
            "terminal_no": terminal_no,
            "terminal_name": terminal_name,
            "terminal_state_name": terminal_state_name,
            "custom_event_id": custom_event_info.get("custom_event_id"),
            "custom_event_name": custom_event_info.get("custom_event_name"),
            "time": custom_event_info.get("time"),
            "elapsed_time": custom_event_info.get("elapsed_time"),
        },
    }
    hist_list_items.append(hist_list_item)
    return hist_list_items