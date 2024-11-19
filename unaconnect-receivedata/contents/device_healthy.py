import os
import boto3
import uuid
import time
import ddb
import textwrap
import mail
from datetime import datetime, timezone, timedelta
from dateutil import relativedelta
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

HIST_LIST_TTL = int(os.environ["HIST_LIST_TTL"])
NOTIFICATION_HIST_TTL = int(os.environ["NOTIFICATION_HIST_TTL"])

logger = Logger()


def device_healthy_recover(req_body, recv_datetime, device_info, current_state_info, group_list, hist_list_table, notification_hist_table, user_table, account_table):
    logger.debug(f"device_healthy開始 device_info={device_info}, ")

    device_healthy_period = device_info.get("device_data", {}).get("config", {}).get("device_healthy_period", 0)
    event_datetime = req_body.get("timestamp", "")

    # デバイスヘルシーチェック情報チェック

    logger.debug(f"device_healthy_period={device_healthy_period}")
    current_state_info["device_healthy_state"] = 0

    # 履歴一覧データ作成
    expire_datetime = int((datetime.fromtimestamp(event_datetime / 1000) + relativedelta.relativedelta(years=HIST_LIST_TTL)).timestamp())
    hist_list_item = {
        "device_id": device_info.get("device_id"),
        "hist_id": str(uuid.uuid4()),
        "event_datetime": event_datetime,
        "expire_datetime": expire_datetime,
        "recv_datetime": recv_datetime,
        "hist_data": {
            "device_name": device_info.get("device_data", {}).get("config", {}).get("device_name"),
            "imei": device_info.get("imei"),
            "sigfox_id": device_info.get("sigfox_id"),
            "group_list": group_list,
            "event_type": "device_unhealthy",
            "device_healthy_period": device_healthy_period,
            "occurrence_flag": 0,
        }
    }

    # 通知設定チェック
    if "notification_settings" not in device_info.get('device_data', {}).get('config', {}) or\
        len(device_info.get('device_data', {}).get('config', {}).get('notification_settings', [])) == 0:
        return current_state_info

    # 通知先チェック
    notification_target_list = device_info.get('device_data', {}).get('config', {}).get('notification_target_list', [])
    if not notification_target_list:
        return current_state_info

    # グループ名
    group_name_list = []
    for group_info in group_list:
        group_name_list.append(group_info.get("group_name"))
    group_name = "、".join(group_name_list)
    logger.debug(f"group_name={group_name}")

    # メール通知設定チェック
    notification_settings_list = device_info.get('device_data', {}).get('config', {}).get('notification_settings', [])
    for notification_settings in notification_settings_list:
        # 初期化
        mail_send_flg = False
        notice_hist_info = {}
        event_detail = ""
 
        # デバイスヘルシー 通知設定判定
        if notification_settings.get('event_trigger') == "device_change" and\
            notification_settings.get('event_type') == "device_unhealthy":

            event_detail = f"""
                　【信号未受信異常（復旧）】
                　デバイスから信号を受信しました。
            """
            mail_send_flg = True

            # メール通知
            if (mail_send_flg):
                mail_address_list = ddb.get_notice_mailaddress(notification_target_list, user_table, account_table)
                JST = timezone(timedelta(hours=+9), 'JST')
                now = datetime.now()
                notice_datetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)
                event_dt = datetime.fromtimestamp(notice_datetime / 1000).replace(tzinfo=timezone.utc).astimezone(tz=JST).strftime('%Y/%m/%d %H:%M:%S')

                mail_subject = "イベントが発生しました"
                event_detail = textwrap.dedent(event_detail)
                device_name = (
                    device_info.get("device_data", {}).get("config", {}).get("device_name")
                    if device_info.get("device_data", {}).get("config", {}).get("device_name")
                    else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{device_info.get("imei")}(IMEI)"
                )
                mail_body = textwrap.dedent(f"""
                    ■発生日時：{event_dt}

                    ■グループ：{group_name}
                    　デバイス：{device_name}

                    ■イベント内容
                """).strip()

                # メール送信
                mail_body = mail_body + event_detail
                logger.debug(f"mail_body={mail_body}")
                mail.send_email(mail_address_list, mail_subject, mail_body)

                # 通知履歴保存
                expire_datetime = int((datetime.fromtimestamp(notice_datetime / 1000) + relativedelta.relativedelta(years=NOTIFICATION_HIST_TTL)).timestamp())
                notice_hist_info = {
                    'notification_hist_id': str(uuid.uuid4()),
                    'contract_id': device_info.get('device_data', {}).get('param', {}).get('contract_id'),
                    'notification_datetime': notice_datetime,
                    'expire_datetime': expire_datetime,
                    'notification_user_list': notification_target_list
                }
                ddb.put_notice_hist(notice_hist_info, notification_hist_table)
                # 履歴一覧編集
                hist_list_item['hist_data']['notification_hist_id'] = notice_hist_info.get("notification_hist_id")
                hist_list_item['event_datetime'] = notice_datetime
                hist_list_expire_datetime = int((datetime.fromtimestamp(notice_datetime / 1000) + relativedelta.relativedelta(years=HIST_LIST_TTL)).timestamp())
                hist_list_item['expire_datetime'] = hist_list_expire_datetime
                break

    ddb.put_db_item(hist_list_item, hist_list_table)

    logger.debug("device_healthy正常終了")
    return current_state_info
