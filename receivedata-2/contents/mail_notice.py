import os
import ddb
import uuid
import time
import textwrap
from datetime import datetime, timezone, timedelta
from dateutil import relativedelta
from aws_lambda_powertools import Logger

import mail

logger = Logger()

NOTIFICATION_HIST_TTL = int(os.environ["NOTIFICATION_HIST_TTL"])


def diNameToState(terminal_state_name, device_info):
    di_list = (
        device_info.get("device_data", {})
        .get("config", {})
        .get("terminal_settings", {})
        .get("di_list", [])
    )
    for di in di_list:
        if di.get("di_on_name") == terminal_state_name:
            di_state = 0
            break
        elif di.get("di_off_name") == terminal_state_name:
            di_state = 1
            break
    return di_state


def automationSetting(event_type, event_detail_state, event_detail_flag):
    # トリガーイベント項目
    if event_type == "di_change_state":
        event_type_label = "接点入力(接点状態)"
    elif event_type == "di_unhealthy":
        event_type_label = "接点入力(変化検出状態)"
    elif event_type == "device_unhealthy":
        event_type_label = "デバイスヘルシー未受信"
    elif event_type == "battery_near":
        event_type_label = "バッテリーニアエンド"
    elif event_type == "device_abnormality":
        event_type_label = "機器異常"
    elif event_type == "parameter_abnormality":
        event_type_label = "パラメータ異常"
    elif event_type == "fw_update_abnormality":
        event_type_label = "FW更新異常"
    elif event_type == "power_on":
        event_type_label = "電源ON"

    # トリガーイベント詳細
    if event_type == "di_change_state":
        if event_detail_state == 1:
            event_detail_label = "オープン"
        else:
            event_detail_label = "クローズ"
    elif event_type == "di_unhealthy":
        if event_detail_flag == 0:
            event_detail_label = "接点入力検出復旧"
        else:
            event_detail_label = "接点入力未変化検出"
    else:
        if event_detail_flag == 0:
            event_detail_label = "正常"
        else:
            event_detail_label = "異常"

    event_label = {"event_type_label": event_type_label, "event_detail_label": event_detail_label}

    return event_label


def mailNotice(hist_list, device_info, user_table, account_table, notification_hist_table):
    logger.debug(f"mailNotice開始 hist_list={hist_list} device_info={device_info}")

    # 履歴一覧チェック
    if len(hist_list) == 0:
        # 履歴一覧が存在しない場合、通知無し応答
        return hist_list

    # 通知設定チェック
    if (
        "notification_settings" not in device_info.get("device_data", {}).get("config", {})
        or len(
            device_info.get("device_data", {}).get("config", {}).get("notification_settings", [])
        )
        == 0
    ):
        # 通知設定が存在しない場合、通知無し応答
        return hist_list

    # 通知先チェック
    notification_target_list = (
        device_info.get("device_data", {}).get("config", {}).get("notification_target_list", [])
    )
    if not notification_target_list:
        return hist_list

    # メール通知設定チェック
    notification_settings_list = (
        device_info.get("device_data", {}).get("config", {}).get("notification_settings", [])
    )
    for notification_settings in notification_settings_list:
        # グループ名
        group_name_list = []
        for group_info in hist_list[0].get("hist_data", {}).get("group_list", []):
            group_name_list.append(group_info.get("group_name"))
        group_name = "、".join(group_name_list)
        logger.debug(f"group_name={group_name}")

        for i, hist_list_data in enumerate(hist_list):
            # 初期化
            mail_send_flg = False
            event_detail = ""

            # 接点入力
            logger.debug(f"event_type={hist_list_data.get('hist_data', {}).get('event_type')}")
            if (
                hist_list_data.get("hist_data", {}).get("event_type") == "di_change"
                and notification_settings.get("event_trigger") == "di_change"
                and notification_settings.get("terminal_no")
                == hist_list_data.get("hist_data", {}).get("terminal_no")
            ):
                di_state = diNameToState(
                    hist_list_data.get("hist_data", {}).get("terminal_state_name"), device_info
                )
                if notification_settings.get("change_detail") == di_state:
                    terminal_name = hist_list_data.get("hist_data", {}).get("terminal_name")
                    terminal_state_name = hist_list_data.get("hist_data", {}).get(
                        "terminal_state_name"
                    )
                    event_detail = f"""
                        　【接点入力変化】
                        　{terminal_name}が{terminal_state_name}に変化しました。
                    """
                    mail_send_flg = True

            # 接点出力
            if (
                hist_list_data.get("hist_data", {}).get("event_type") == "do_change"
                and notification_settings.get("event_trigger") == "do_change"
                and notification_settings.get("terminal_no")
                == hist_list_data.get("hist_data", {}).get("terminal_no")
            ):
                terminal_name = hist_list_data.get("hist_data", {}).get("terminal_name")
                terminal_state_name = hist_list_data.get("hist_data", {}).get(
                    "terminal_state_name"
                )
                event_detail = f"""
                    　【接点出力変化】
                    　{terminal_name}が{terminal_state_name}に変化しました。
                """
                mail_send_flg = True

            # デバイス状態（バッテリーニアエンド）
            elif (
                hist_list_data.get("hist_data", {}).get("event_type") == "battery_near"
                and notification_settings.get("event_trigger") == "device_change"
                and notification_settings.get("event_type") == "battery_near"
            ):
                if hist_list_data.get("hist_data", {}).get("occurrence_flag") == 1:
                    event_detail = f"""
                        　【電池残量変化（少ない）】
                        　デバイスの電池残量が少ない状態に変化しました。
                    """
                else:
                    event_detail = f"""
                        　【電池残量変化（十分）】
                        　デバイスの電池残量が十分な状態に変化しました。
                    """
                mail_send_flg = True

            # デバイス状態（機器異常）
            elif (
                hist_list_data.get("hist_data", {}).get("event_type") == "device_abnormality"
                and notification_settings.get("event_trigger") == "device_change"
                and notification_settings.get("event_type") == "device_abnormality"
            ):
                if hist_list_data.get("hist_data", {}).get("occurrence_flag") == 1:
                    event_detail = f"""
                        　【機器異常（発生）】
                        　機器異常が発生しました。
                    """
                else:
                    event_detail = f"""
                        　【機器異常（復旧）】
                        　機器異常が復旧しました。
                    """
                mail_send_flg = True

            # デバイス状態（パラメータ異常）
            elif (
                hist_list_data.get("hist_data", {}).get("event_type") == "parameter_abnormality"
                and notification_settings.get("event_trigger") == "device_change"
                and notification_settings.get("event_type") == "parameter_abnormality"
            ):
                if hist_list_data.get("hist_data", {}).get("occurrence_flag") == 1:
                    event_detail = f"""
                        　【パラメータ異常（発生）】
                        　パラメータ異常が発生しました。
                    """
                else:
                    event_detail = f"""
                        　【パラメータ異常（復旧）】
                        　パラメータ異常が復旧しました。
                    """
                mail_send_flg = True

            # デバイス状態（FW更新異常）
            elif (
                hist_list_data.get("hist_data", {}).get("event_type") == "fw_update_abnormality"
                and notification_settings.get("event_trigger") == "device_change"
                and notification_settings.get("event_type") == "fw_update_abnormality"
            ):
                if hist_list_data.get("hist_data", {}).get("occurrence_flag") == 1:
                    event_detail = f"""
                        　【FW更新異常（発生）】
                        　FW更新異常が発生しました。
                    """
                else:
                    event_detail = f"""
                        　【FW更新異常（復旧）】
                        　FW更新異常が復旧しました。
                    """
                mail_send_flg = True

            # 電源ON
            elif (
                hist_list_data.get("hist_data", {}).get("event_type") == "power_on"
                and notification_settings.get("event_trigger") == "device_change"
                and notification_settings.get("event_type") == "power_on"
            ):
                event_detail = f"""
                    　【電源ON】
                    　デバイスの電源がONになりました。
                """
                mail_send_flg = True

            # 遠隔制御応答
            elif (
                hist_list_data.get("hist_data", {}).get("event_type")
                in [
                    "manual_control",
                    "timer_control",
                    "on_timer_control",
                    "off_timer_control",
                    "automation_control",
                    "on_automation_control",
                    "off_automation_control",
                ]
                and notification_settings.get("event_trigger") == "do_change"
                and notification_settings.get("terminal_no") == hist_list_data.get("hist_data", {}).get("terminal_no")
            ):
                if "link_terminal_no" in hist_list_data.get("hist_data", {}):
                    if (
                        hist_list_data.get("hist_data", {}).get("event_type")
                        == "manual_control"
                    ):
                        terminal_name = hist_list_data.get("hist_data", {}).get(
                            "terminal_name"
                        )
                        link_terminal_name = hist_list_data.get("hist_data", {}).get(
                            "link_terminal_name"
                        )
                        link_terminal_state_name = hist_list_data.get("hist_data", {}).get(
                            "link_terminal_state_name"
                        )
                        control_exec_user_name = hist_list_data.get("hist_data", {}).get(
                            "control_exec_user_name"
                        )
                        event_detail = f"""
                            　【マニュアルコントロール(成功)】
                            　{terminal_name}のコントロールコマンドがデバイスに届き、{link_terminal_name}が{link_terminal_state_name}に変化しました。
                            　 ※{control_exec_user_name}が操作
                        """
                    elif hist_list_data.get("hist_data", {}).get("event_type") in [
                        "timer_control",
                        "on_timer_control",
                        "off_timer_control",
                    ]:
                        terminal_name = hist_list_data.get("hist_data", {}).get(
                            "terminal_name"
                        )
                        link_terminal_name = hist_list_data.get("hist_data", {}).get(
                            "link_terminal_name"
                        )
                        link_terminal_state_name = hist_list_data.get("hist_data", {}).get(
                            "link_terminal_state_name"
                        )
                        timer_time = hist_list_data.get("hist_data", {}).get("timer_time")
                        do_timer_name = hist_list_data.get("hist_data", {}).get("do_timer_name")
                        event_detail = f"""
                            　【スケジュール(成功)】
                            　{terminal_name}のコントロールコマンドがデバイスに届き、{link_terminal_name}が{link_terminal_state_name}に変化しました。
                            　 ※スケジュール「{do_timer_name} ／ {timer_time}」
                        """
                    else:
                        terminal_name = hist_list_data.get("hist_data", {}).get(
                            "terminal_name"
                        )
                        link_terminal_name = hist_list_data.get("hist_data", {}).get(
                            "link_terminal_name"
                        )
                        link_terminal_state_name = hist_list_data.get("hist_data", {}).get(
                            "link_terminal_state_name"
                        )
                        automation_trigger_imei = hist_list_data.get("hist_data", {}).get(
                            "automation_trigger_imei"
                        )
                        automation_trigger_device_name = (
                            hist_list_data.get("hist_data", {}).get("automation_trigger_device_name")
                            if hist_list_data.get("hist_data", {}).get("automation_trigger_device_name")
                            else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{automation_trigger_imei}（IMEI）"
                        )
                        automation_trigger_event_type = hist_list_data.get(
                            "hist_data", {}
                        ).get("automation_trigger_event_type")
                        automation_trigger_event_detail_state = hist_list_data.get(
                            "hist_data", {}
                        ).get("automation_trigger_event_detail_state")
                        automation_trigger_event_detail_flag = hist_list_data.get(
                            "hist_data", {}
                        ).get("automation_trigger_event_detail_flag")
                        event_label = automationSetting(
                            automation_trigger_event_type,
                            automation_trigger_event_detail_state,
                            automation_trigger_event_detail_flag,
                        )
                        event_detail = f"""
                            　【オートメーション(成功)】
                            　{terminal_name}コントロールコマンドがデバイスに届き、{link_terminal_name}が{link_terminal_state_name}に変化しました。
                            　 ※オートメーション「{automation_trigger_device_name} ／ {event_label["event_type_label"]} ／ {event_label["event_detail_label"]}」
                        """
                    mail_send_flg = True
                else:
                    if hist_list_data.get("hist_data", {}).get("event_type") == "manual_control":
                        terminal_name = hist_list_data.get("hist_data", {}).get(
                            "terminal_name"
                        )
                        control_exec_user_name = hist_list_data.get("hist_data", {}).get(
                            "control_exec_user_name"
                        )
                        if hist_list_data.get("hist_data", {}).get("control_result") == "not_excuted_link":
                            terminal_name = hist_list_data.get("hist_data", {}).get("terminal_name")
                            timer_time = hist_list_data.get("hist_data", {}).get("timer_time")
                            event_detail = f"""
                                　【マニュアルコントロール(実施中)】
                                　{terminal_name}のコントロールコマンドがデバイスに届きました。
                                　 ※{control_exec_user_name}が操作
                            """
                        else:
                            event_detail = f"""
                                　【マニュアルコントロール(コマンド送信)】
                                　{terminal_name}のコントロールコマンドがデバイスに届きました。
                                　 ※{control_exec_user_name}が操作
                            """
                        mail_send_flg = True
                    elif hist_list_data.get("hist_data", {}).get("event_type") in [
                        "timer_control",
                        "on_timer_control",
                        "off_timer_control",
                    ]:
                        if hist_list_data.get("hist_data", {}).get("control_result") == "not_excuted_link":
                            terminal_name = hist_list_data.get("hist_data", {}).get("terminal_name")
                            timer_time = hist_list_data.get("hist_data", {}).get("timer_time")
                            do_timer_name = hist_list_data.get("hist_data", {}).get("do_timer_name")
                            event_detail = f"""
                                　【スケジュール(実施中)】
                                　{terminal_name}のコントロールコマンドがデバイスに届きました。
                                　 ※スケジュール「{do_timer_name} ／ {timer_time}」
                            """
                        else:
                            terminal_name = hist_list_data.get("hist_data", {}).get("terminal_name")
                            timer_time = hist_list_data.get("hist_data", {}).get("timer_time")
                            do_timer_name = hist_list_data.get("hist_data", {}).get("do_timer_name")
                            event_detail = f"""
                                　【スケジュール(コマンド送信)】
                                　{terminal_name}のコントロールコマンドがデバイスに届きました。
                                　 ※スケジュール「{do_timer_name} ／ {timer_time}」
                            """
                        mail_send_flg = True
                    else:
                        terminal_name = hist_list_data.get("hist_data", {}).get("terminal_name")
                        automation_trigger_imei = hist_list_data.get("hist_data", {}).get(
                            "automation_trigger_imei"
                        )
                        automation_trigger_device_name = (
                            hist_list_data.get("hist_data", {}).get("automation_trigger_device_name")
                            if hist_list_data.get("hist_data", {}).get("automation_trigger_device_name")
                            else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{automation_trigger_imei}（IMEI）"
                        )
                        automation_trigger_event_type = hist_list_data.get("hist_data", {}).get(
                            "automation_trigger_event_type"
                        )
                        automation_trigger_event_detail_state = hist_list_data.get(
                            "hist_data", {}
                        ).get("automation_trigger_event_detail_state")
                        automation_trigger_event_detail_flag = hist_list_data.get(
                            "hist_data", {}
                        ).get("automation_trigger_event_detail_flag")
                        event_label = automationSetting(
                            automation_trigger_event_type,
                            automation_trigger_event_detail_state,
                            automation_trigger_event_detail_flag,
                        )
                        if hist_list_data.get("hist_data", {}).get("control_result") == "not_excuted_link":
                            event_detail = f"""
                                　【オートメーション(実施中)】
                                　{terminal_name}コントロールコマンドがデバイスに届きました。
                                　 ※オートメーション「{automation_trigger_device_name} ／ {event_label["event_type_label"]} ／ {event_label["event_detail_label"]}」
                            """
                        else:
                            event_detail = f"""
                                　【オートメーション(コマンド送信)】
                                　{terminal_name}コントロールコマンドがデバイスに届きました。
                                　 ※オートメーション「{automation_trigger_device_name} ／ {event_label["event_type_label"]} ／ {event_label["event_detail_label"]}」
                            """
                        mail_send_flg = True

            # デバイスヘルシー 通知設定判定
            elif hist_list_data.get('hist_data', {}).get('event_type') == "device_unhealthy" and \
                notification_settings.get('event_trigger') == "device_change" and\
                notification_settings.get('event_type') == "device_unhealthy":
                event_detail = f"""
                    　【信号未受信異常（復旧）】
                    　デバイスから信号を受信しました。
                """
                mail_send_flg = True

            elif hist_list_data.get('hist_data', {}).get('event_type') == "di_unhealthy" and \
                notification_settings.get('event_trigger') == "di_change" and\
                notification_settings.get('terminal_no') == hist_list_data.get('hist_data', {}).get('terminal_no') and\
                notification_settings.get('change_detail') == 2:
                terminal_name = hist_list_data.get('hist_data', {}).get('terminal_name')
                event_detail = f"""
                    　【接点入力未変化検出（復旧）】
                    　{terminal_name}の変化信号を受信しました。
                """
                mail_send_flg = True

            # メール通知
            if mail_send_flg:
                mail_address_list = ddb.get_notice_mailaddress(
                    notification_target_list, user_table, account_table
                )
                now = datetime.now()
                szNoticeDatetime = int(time.mktime(now.timetuple()) * 1000) + int(
                    now.microsecond / 1000
                )
                szExpireDatetime = int(
                    (
                        datetime.fromtimestamp(szNoticeDatetime / 1000)
                        + relativedelta.relativedelta(years=NOTIFICATION_HIST_TTL)
                    ).timestamp()
                )
                JST = timezone(timedelta(hours=+9), "JST")
                event_dt = (
                    datetime.fromtimestamp(int(hist_list_data.get("event_datetime")) / 1000)
                    .replace(tzinfo=timezone.utc)
                    .astimezone(tz=JST)
                    .strftime("%Y/%m/%d %H:%M:%S")
                )

                mail_subject = "イベントが発生しました"
                event_detail = textwrap.dedent(event_detail)
                device_name = (
                    hist_list_data.get('hist_data', {}).get("device_name")
                    if hist_list_data.get('hist_data', {}).get("device_name")
                    else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{device_info.get("imei")}（IMEI）"
                )
                mail_body = textwrap.dedent(
                    f"""
                    ■発生日時：{event_dt}

                    ■グループ：{group_name}
                    　デバイス：{device_name}

                    ■イベント内容
                """
                ).strip()

                # メール送信
                mail_body = mail_body + event_detail
                logger.debug(f"mail_body={mail_body}")
                mail.send_email(mail_address_list, mail_subject, mail_body)

                # 通知履歴保存
                notice_hist_info = {
                    "notification_hist_id": str(uuid.uuid4()),
                    "contract_id": device_info.get("device_data", {})
                    .get("param", {})
                    .get("contract_id"),
                    "notification_datetime": szNoticeDatetime,
                    "expire_datetime": szExpireDatetime,
                    "notification_user_list": notification_target_list,
                }
                ddb.put_notice_hist(notice_hist_info, notification_hist_table)

                # 履歴一覧編集
                hist_list[i]["hist_data"]["notification_hist_id"] = notice_hist_info[
                    "notification_hist_id"
                ]

    logger.debug(f"mailNotice終了 hist_list={hist_list}")
    return hist_list
