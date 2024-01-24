import ddb
import uuid
import time
import textwrap
from datetime import datetime, timezone, timedelta
from aws_lambda_powertools import Logger

import mail

logger = Logger()


def diNameToState(terminal_state_name, device_info):
    di_list = device_info["device_data"]["config"]["terminal_settings"]["di_list"]
    for di in di_list:
        if di["di_on_name"] == terminal_state_name:
            di_state = 1
            break
        elif di["di_off_name"] == terminal_state_name:
            di_state = 0
            break
    return di_state


def mailNotice(hist_list, device_info, user_table, account_table, notification_hist_table):
    logger.debug(f"mailNotice開始 hist_list={hist_list} device_info={device_info}")

    # 履歴一覧チェック
    if len(hist_list) == 0:
        # 履歴一覧が存在しない場合、通知無し応答
        return hist_list

    # 通知設定チェック
    if "notification_settings" not in device_info['device_data']['config'] or\
          len(device_info['device_data']['config']['notification_settings']) == 0:
        # 通知設定が存在しない場合、通知無し応答
        return hist_list

    # メール通知設定チェック
    notification_settings_list = device_info['device_data']['config']['notification_settings']
    for notification_settings in notification_settings_list:
        # 通知先チェック
        if len(notification_settings['notification_target_list']) == 0:
            # 通知先が存在しないため、スキップ
            continue

        # グループ名
        group_name_list = []
        for group_info in hist_list[0]['hist_data']['group_list']:
            group_name_list.append(group_info.get("group_name"))
        group_name = "、".join(group_name_list)
        logger.debug(f"group_name={group_name}")

        for i, hist_list_data in enumerate(hist_list):
            # 初期化
            mail_send_flg = False
            event_detail = ""

            # 接点入力
            logger.debug(f"event_type={hist_list_data['hist_data']['event_type']}")
            if hist_list_data['hist_data']['event_type'] == "di_change" and notification_settings['event_trigger'] == "di_change" and\
                notification_settings['terminal_no'] == hist_list_data['hist_data']['terminal_no']:
                di_state = diNameToState(hist_list_data['hist_data']['terminal_state_name'], device_info)
                if notification_settings['change_detail'] == di_state:
                    event_detail = f"""\
                        【接点入力変化】
                        {hist_list_data['hist_data']['terminal_name']}が{hist_list_data['hist_data']['terminal_state_name']}に変化しました。
                    """
                    mail_send_flg = True

            # 接点出力
            if hist_list_data['hist_data']['event_type'] == "do_change" and notification_settings['event_trigger'] == "do_change" and\
                notification_settings['terminal_no'] == hist_list_data['hist_data']['terminal_no']:
                event_detail = f"""\
                    【接点出力変化】
                    {hist_list_data['hist_data']['terminal_name']}が{hist_list_data['hist_data']['terminal_state_name']}に変化しました。
                """
                mail_send_flg = True

            # デバイス状態（バッテリーニアエンド）
            elif hist_list_data['hist_data']['event_type'] == "battery_near" and notification_settings['event_trigger'] == "device_change" and\
                notification_settings['event_type'] == "battery_near":
                if hist_list_data['hist_data']['occurrence_flag'] == 1:
                    event_detail = f"""\
                        【電池残量変化（少ない）】
                        デバイスの電池残量が少ない状態に変化しました。
                    """
                else:
                    event_detail = f"""\
                        【電池残量変化（十分）】
                        デバイスの電池残量が十分な状態に変化しました。
                    """
                mail_send_flg = True

            # デバイス状態（機器異常）
            elif hist_list_data['hist_data']['event_type'] == "device_abnormality" and notification_settings['event_trigger'] == "device_change" and\
                notification_settings['event_type'] == "device_abnormality":
                if hist_list_data['hist_data']['occurrence_flag'] == 1:
                    event_detail = f"""\
                        【機器異常（発生）】
                        機器異常が発生しました。
                    """
                else:
                    event_detail = f"""\
                        【機器異常（復旧）】
                        機器異常が復旧しました。
                    """
                mail_send_flg = True

            # デバイス状態（パラメータ異常）
            elif hist_list_data['hist_data']['event_type'] == "parameter_abnormality" and notification_settings['event_trigger'] == "device_change" and\
                notification_settings['event_type'] == "parameter_abnormality":
                if hist_list_data['hist_data']['occurrence_flag'] == 1:
                    event_detail = f"""\
                        【パラメータ異常（発生）】
                        パラメータ異常が発生しました。
                    """
                else:
                    event_detail = f"""\
                        【パラメータ異常（復旧）】
                        パラメータ異常が復旧しました。
                    """
                mail_send_flg = True

            # デバイス状態（FW更新異常）
            elif hist_list_data['hist_data']['event_type'] == "fw_update_abnormality" and notification_settings['event_trigger'] == "device_change" and\
                notification_settings['event_type'] == "fw_update_abnormality":
                if hist_list_data['hist_data']['occurrence_flag'] == 1:
                    event_detail = f"""\
                        【FW更新異常（発生）】
                        FW更新異常が発生しました。
                    """
                else:
                    event_detail = f"""\
                        【FW更新異常（復旧）】
                        FW更新異常が復旧しました。
                    """
                mail_send_flg = True

            # 電源ON
            elif hist_list_data['hist_data']['event_type'] == "power_on" and notification_settings['event_trigger'] == "device_change" and\
                notification_settings['event_type'] == "power_on":
                event_detail = f"""\
                    【電源ON】
                    デバイスの電源がONになりました。
                """
                mail_send_flg = True

            # 遠隔制御応答
            elif hist_list_data['hist_data']['event_type'] in ["manual_control", "on_timer_control", "off_timer_control"] and\
                notification_settings['event_trigger'] == "do_change":
                if "link_terminal_no" in hist_list_data['hist_data']:
                    if notification_settings['terminal_no'] == hist_list_data["hist_data"]["terminal_no"]:
                        if hist_list_data['hist_data']['event_type'] == "manual_control":
                            event_detail = f"""\
                                【画面操作による制御(成功)】
                                制御信号（{hist_list_data['hist_data']['terminal_name']}）がデバイスに届き、\
                                    {hist_list_data['hist_data']['link_terminal_name']}が{hist_list_data['hist_data']['link_terminal_state_name']}に変化しました。
                                ※{hist_list_data["hist_data"]["control_exec_user_name"]}が操作を行いました。
                            """
                        else:
                            if hist_list_data['hist_data']['event_type'] == "on_timer_control":
                                control = "ON制御"
                            else:
                                control = "OFF制御"
                            event_detail = f"""\
                                【タイマー設定による制御(成功)】
                                制御信号（{hist_list_data['hist_data']['terminal_name']}）がデバイスに届き、\
                                    {hist_list_data['hist_data']['link_terminal_name']}が{hist_list_data['hist_data']['link_terminal_state_name']}に変化しました。
                                ※タイマー設定「{control} {hist_list_data['hist_data']['timer_time']}」により制御信号を送信しました。
                            """
                        mail_send_flg = True
                else:
                    if hist_list_data["hist_data"]["terminal_no"] == notification_settings["terminal_no"]:
                        event_detail = f"""\
                            【画面操作による制御(成功)】
                            {hist_list_data['hist_data']['terminal_name']}の制御信号がデバイスに届きました。
                            ※{hist_list_data["hist_data"]["control_exec_user_name"]}が操作を行いました。
                        """
                        mail_send_flg = True

            # メール通知
            if (mail_send_flg):
                mail_address_list = ddb.get_notice_mailaddress(notification_settings['notification_target_list'], user_table, account_table)
                now = datetime.now()
                szNoticeDatetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)
                JST = timezone(timedelta(hours=+9), 'JST')
                event_dt = datetime.fromtimestamp(int(hist_list_data['event_datetime']) / 1000).replace(tzinfo=timezone.utc).astimezone(tz=JST).strftime('%Y/%m/%d %H:%M:%S')
                recv_dt = datetime.fromtimestamp(int(hist_list_data['recv_datetime']) / 1000).replace(tzinfo=timezone.utc).astimezone(tz=JST).strftime('%Y/%m/%d %H:%M:%S')

                mail_subject = "イベントが発生しました"
                event_detail = event_detail.strip()
                mail_body = f"""\
                    ■発生日時：{event_dt}
                    　受信日時：{recv_dt}

                    ■グループ：{group_name}
                    　デバイス：{hist_list_data['hist_data']['device_name']}

                    ■イベント内容
                    {event_detail}
                """

                # 招待メール送信
                logger.debug(f"mail_body={mail_body}")
                mail.send_email(mail_address_list, mail_subject, textwrap.dedent(mail_body))

                # 通知履歴保存
                notice_hist_info = {
                    'notification_hist_id': str(uuid.uuid4()),
                    'contract_id': device_info['device_data']['param']['contract_id'],
                    'notification_datetime': szNoticeDatetime,
                    'notification_user_list': notification_settings['notification_target_list']
                }
                ddb.put_notice_hist(notice_hist_info, notification_hist_table)

                # 履歴一覧編集
                hist_list[i]['hist_data']['notification_hist_id'] = notice_hist_info['notification_hist_id']

    logger.debug(f"mailNotice終了 hist_list={hist_list}")
    return hist_list
