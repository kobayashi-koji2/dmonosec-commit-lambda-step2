import os
import ddb
import uuid
import time
import textwrap
from datetime import datetime, timezone, timedelta
from dateutil import relativedelta
from aws_lambda_powertools import Logger

import mail

NOTIFICATION_HIST_TTL = int(os.environ["NOTIFICATION_HIST_TTL"])
HIST_LIST_TTL = int(os.environ["HIST_LIST_TTL"])

logger = Logger()


def convert_to_full_width(number):
    normal_numbers = "0123456789"
    full_width_numbers = "０１２３４５６７８９"
    trans_table = str.maketrans(normal_numbers, full_width_numbers)
    return str(number).translate(trans_table)


def mailNotice(device_info, group_name, hist_list_items, now_datetime, user_table, account_table, notification_hist_table):
    logger.debug(f"mailNotice開始 device_info={device_info} hist_list_items={hist_list_items}")

    # 通知設定チェック
    if "notification_settings" not in device_info.get('device_data', {}).get('config', {}) or\
        len(device_info.get('device_data', {}).get('config', {}).get('notification_settings', [])) == 0:
        return hist_list_items

    # 通知先チェック
    notification_target_list = device_info.get('device_data', {}).get('config', {}).get('notification_target_list', [])
    if not notification_target_list:
        return hist_list_items

    # メール通知設定チェック
    notification_settings_list = device_info.get('device_data', {}).get('config', {}).get('notification_settings', [])
    for notification_settings in notification_settings_list:
        for i, hist_list_item in enumerate(hist_list_items):

            # 初期化
            mail_send_flg = False
            notice_hist_info = {}
            event_detail = ""

            # カスタムイベント 通知設定判定
            if notification_settings.get('event_trigger') == "custom_event" and\
               notification_settings.get('custom_event_id') == hist_list_item.get('hist_data', {}).get('custom_event_id'):

                terminal_name = hist_list_item.get('hist_data', {}).get('terminal_name')
                terminal_state_name = hist_list_item.get('hist_data', {}).get('terminal_state_name')
                custom_event_name = hist_list_item.get('hist_data', {}).get('custom_event_name')

                # 日時指定
                if notification_settings.get('event_type') == "custom_datetime":
                    custom_time = hist_list_item.get('hist_data', {}).get('time')
                    event_detail = f"""
                        　【日時状態お知らせイベント】
                        　{custom_time}時点の{terminal_name}の状態は{terminal_state_name}でした。
                        　※カスタムイベント「{custom_event_name}」
                    """
                # 経過時間
                else:
                    elapsed_time = hist_list_item.get('hist_data', {}).get('elapsed_time')
                    event_detail = f"""
                        　【状態継続お知らせイベント】
                        　{terminal_name}が{terminal_state_name}に変化し、{elapsed_time}分間が経過しました。
                        　※カスタムイベント「{custom_event_name}」
                    """
                mail_send_flg = True

            # メール通知
            if (mail_send_flg):
                mail_address_list = ddb.get_notice_mailaddress(notification_target_list, user_table, account_table)
                JST = timezone(timedelta(hours=+9), 'JST')
                now = datetime.now()
                notice_datetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)
                event_dt = datetime.fromtimestamp(int(int(hist_list_items[i]['event_datetime']) / 1000)).replace(tzinfo=timezone.utc).astimezone(tz=JST).strftime('%Y/%m/%d %H:%M:%S')

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
                hist_list_items[i]['hist_data']['notification_hist_id'] = notice_hist_info.get("notification_hist_id")
                break

    logger.debug("mailNotice終了")
    return hist_list_items
