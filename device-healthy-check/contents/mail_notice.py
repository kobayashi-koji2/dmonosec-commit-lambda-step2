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
            healthy_state = hist_list_item.get('hist_data', {}).get('occurrence_flag')

            # デバイスヘルシー 通知設定判定
            if hist_list_item.get('hist_data', {}).get('event_type') == "device_unhealthy" and \
                notification_settings.get('event_trigger') == "device_change" and\
                notification_settings.get('event_type') == "device_unhealthy":

                if healthy_state == 0:
                    event_detail = f"""
                        　【信号未受信異常（復旧）】
                        　デバイスから信号を受信しました。
                    """
                else:
                    device_healthy_period = device_info.get('device_data', {}).get('config', {}).get('device_healthy_period', {})
                    full_width_period = convert_to_full_width(device_healthy_period)
                    event_detail = f"""
                        　【信号未受信異常（発生）】
                        　設定した期間({full_width_period}日)、デバイスから信号を受信しませんでした。
                    """
                mail_send_flg = True


            # 接点入力未変化検出 通知設定判定
            elif hist_list_item.get('hist_data', {}).get('event_type') == "di_unhealthy" and \
                notification_settings.get('event_trigger') == "di_change" and\
                notification_settings.get('terminal_no') == hist_list_item.get('hist_data', {}).get('terminal_no') and\
                notification_settings.get('change_detail') == 2:

                terminal_name = hist_list_item.get('hist_data', {}).get('terminal_name')
                if healthy_state == 0:
                    event_detail = f"""
                        　【接点入力未変化検出（復旧）】
                        　{terminal_name}の変化信号を受信しました。
                    """
                else:
                    di_healthy_period = hist_list_item.get('hist_data', {}).get('di_healthy_period')
                    di_healthy_type = hist_list_item.get('hist_data', {}).get('di_healthy_type')
                    if di_healthy_type == "hour":
                        full_width_period = convert_to_full_width(di_healthy_period) + "時間"
                    else:
                        full_width_period = convert_to_full_width(di_healthy_period) + "日"
                    event_detail = f"""
                        　【接点入力未変化検出（発生）】
                        　設定した期間({full_width_period})、{terminal_name}の変化信号を受信しませんでした。
                    """
                mail_send_flg = True

            # メール通知
            if (mail_send_flg):
                mail_address_list = ddb.get_notice_mailaddress(notification_target_list, user_table, account_table)
                JST = timezone(timedelta(hours=+9), 'JST')
                event_dt = datetime.fromtimestamp(now_datetime / 1000).replace(tzinfo=timezone.utc).astimezone(tz=JST).strftime('%Y/%m/%d %H:%M:%S')

                mail_subject = "イベントが発生しました"
                event_detail = textwrap.dedent(event_detail)
                mail_body = textwrap.dedent(f"""
                    ■発生日時：{event_dt}

                    ■グループ：{group_name}
                    　デバイス：{device_info.get("device_data", {}).get("config", {}).get("device_name", device_info.get("imei"))}

                    ■イベント内容
                """).strip()

                # メール送信
                mail_body = mail_body + event_detail
                logger.debug(f"mail_body={mail_body}")
                mail.send_email(mail_address_list, mail_subject, mail_body)

                # 通知履歴保存
                expire_datetime = int((datetime.fromtimestamp(now_datetime / 1000) + relativedelta.relativedelta(years=NOTIFICATION_HIST_TTL)).timestamp())
                notice_hist_info = {
                    'notification_hist_id': str(uuid.uuid4()),
                    'contract_id': device_info.get('device_data', {}).get('param', {}).get('contract_id'),
                    'notification_datetime': now_datetime,
                    'expire_datetime': expire_datetime,
                    'notification_user_list': notification_target_list
                }
                ddb.put_notice_hist(notice_hist_info, notification_hist_table)
                # 履歴一覧編集
                hist_list_items[i]['hist_data']['notification_hist_id'] = notice_hist_info.get("notification_hist_id")
                break

    logger.debug("mailNotice終了")
    return hist_list_items
