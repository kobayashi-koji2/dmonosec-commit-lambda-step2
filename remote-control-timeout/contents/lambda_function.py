import json
import os
import traceback
from decimal import Decimal
from datetime import datetime
from zoneinfo import ZoneInfo
import time
import textwrap

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
import boto3
from botocore.exceptions import ClientError

import auth
import ssm
import db
import convert
import mail
import ddb
import validate

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]


def send_mail(
    notification_setting,
    device,
    remote_control,
    account_table,
    user_table,
    group_table,
    device_relation_table,
    notification_hist_table,
    change_state_mail,
):
    send_datetime = datetime.now(ZoneInfo("Asia/Tokyo"))
    device_name = (
        device.get("device_data", {}).get("config", {}).get("device_name")
        if device.get("device_data", {}).get("config", {}).get("device_name")
        else f"【{device.get("device_data", {}).get("param", {}).get("device_code")}】{device.get("imei")}(IMEI)"
    )
    group_id_list = db.get_device_relation_group_id_list(
        device["device_id"], device_relation_table
    )
    group_name_list = []
    for group_id in group_id_list:
        group_info = db.get_group_info(group_id, group_table)
        group_name_list.append(
            group_info.get("group_data", {}).get("config", {}).get("group_name")
        )
    if group_name_list:
        group_name_list.sort()
    group_name = "、".join(group_name_list)

    do_no = remote_control.get("do_no")
    do = [
        do
        for do in device.get("device_data", {})
        .get("config", {})
        .get("terminal_settings", {})
        .get("do_list", [])
        if do.get("do_no") == do_no
    ]
    do_name = do[0].get("do_name") if do and do[0].get("do_name") else f"接点出力{do_no}"
    user_name = (
        remote_control.get("control_exec_user_name")
        if remote_control.get("control_exec_user_name")
        else remote_control.get("control_exec_user_email_address")
    )

    # メール送信
    mail_to_list = []
    for user_id in (
        device.get("device_data", {}).get("config", {}).get("notification_target_list", [])
    ):
        logger.debug(user_id)
        mail_user = db.get_user_info_by_user_id(user_id, user_table)
        mail_account = db.get_account_info_by_account_id(mail_user["account_id"], account_table)
        mail_to_list.append(mail_account.get("email_address"))

    event_detail = ""
    if not change_state_mail:
        if remote_control.get("control_trigger") == "manual_control":
            event_detail = f"""
                　【マニュアルコントロール(失敗)】
                　{do_name}のコントロールコマンドがデバイスに届かなかった可能性があります。
                　 ※{user_name}が操作
            """
        elif remote_control.get("control_trigger") in ["timer_control", "on_timer_control", "off_timer_control"]:
            event_detail = f"""
                　【スケジュール(失敗)】
                　{do_name}のコントロールコマンドがデバイスに届きませんでした。
                　 ※スケジュール「{remote_control.get("do_timer_name")} ／ {remote_control.get("timer_time")}」
            """
        elif remote_control.get("control_trigger") in [
            "automation_control",
            "on_automation_control",
            "off_automation_control",
        ]:
            trigger_event_type = remote_control.get("automation_trigger_event_type")
            trigger_event_type_name = ""
            trigger_event_detail_name = ""
            if trigger_event_type == "di_change_state":
                trigger_event_type_name = (
                    f"接点入力{remote_control.get('automation_trigger_terminal_no')}（接点状態）"
                )
                trigger_event_detail_name = (
                    "オープン"
                    if remote_control.get("automation_trigger_event_detail_state") == "1"
                    else "クローズ"
                )
            elif trigger_event_type == "di_unhealthy":
                trigger_event_type_name = f"接点入力{remote_control.get('automation_trigger_terminal_no')}（変化検出状態）"
                trigger_event_detail_name = (
                    "接点入力検出復旧"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "接点入力未変化検出"
                )
            elif trigger_event_type == "device_unhealthy":
                trigger_event_type_name = "デバイスヘルシー未受信"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "battery_near":
                trigger_event_type_name = "バッテリーニアエンド"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "device_abnormality":
                trigger_event_type_name = "機器異常"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "parameter_abnormality":
                trigger_event_type_name = "パラメータ異常"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "fw_update_abnormality":
                trigger_event_type_name = "FW更新異常"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "power_on":
                trigger_event_type_name = "電源ON"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            trigger_device_name = (
                remote_control.get("automation_trigger_device_name")
                if remote_control.get("automation_trigger_device_name")
                else f"【{device.get("device_data", {}).get("param", {}).get("device_code")}】{remote_control.get("automation_trigger_imei")}(IMEI)"
            )
            event_detail = f"""
                　【オートメーション(失敗)】
                　{do_name}コントロールコマンドがデバイスに届きませんでした。
                　 ※オートメーション「{trigger_device_name} ／ {trigger_event_type_name} ／ {trigger_event_detail_name}」
            """
    else:
        di_no = remote_control.get("link_di_no")
        di = [
            di
            for di in device.get("device_data", {})
            .get("config", {})
            .get("terminal_settings", {})
            .get("di_list", [])
            if di.get("di_no") == di_no
        ]
        di_name = di[0].get("di_name") if di and di[0].get("di_name") else f"接点入力{di_no}"
        if remote_control.get("control_trigger") == "manual_control":
            event_detail = f"""
                　【マニュアルコントロール(失敗)】
                　{do_name}のコントロールコマンドがデバイスに届きましたが、{di_name}が変化しませんでした。
                　 ※{user_name}が操作
            """
        elif remote_control.get("control_trigger") in ["timer_control", "on_timer_control", "off_timer_control"]:
            event_detail = f"""
                　【スケジュール(失敗)】
                　{do_name}のコントロールコマンドがデバイスに届きましたが、{di_name}が変化しませんでした。
                　 ※スケジュール「{remote_control.get("do_timer_name")} ／ {remote_control.get("link_terminal_state_name")}コントロール ／ {remote_control.get("timer_time")}」」
            """
        elif remote_control.get("control_trigger") in [
            "automation_control",
            "on_automation_control",
            "off_automation_control",
        ]:
            trigger_event_type = remote_control.get("automation_trigger_event_type")
            trigger_event_type_name = ""
            trigger_event_detail_name = ""
            if trigger_event_type == "di_change_state":
                trigger_event_type_name = (
                    f"接点入力{remote_control.get('automation_trigger_terminal_no')}（接点状態）"
                )
                trigger_event_detail_name = (
                    "オープン"
                    if remote_control.get("automation_trigger_event_detail_state") == "1"
                    else "クローズ"
                )
            elif trigger_event_type == "di_unhealthy":
                trigger_event_type_name = f"接点入力{remote_control.get('automation_trigger_terminal_no')}（変化検出状態）"
                trigger_event_detail_name = (
                    "接点入力検出復旧"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "接点入力未変化検出"
                )
            elif trigger_event_type == "device_unhealthy":
                trigger_event_type_name = "デバイスヘルシー未受信"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "battery_near":
                trigger_event_type_name = "バッテリーニアエンド"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "device_abnormality":
                trigger_event_type_name = "機器異常"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "parameter_abnormality":
                trigger_event_type_name = "パラメータ異常"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "fw_update_abnormality":
                trigger_event_type_name = "FW更新異常"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            elif trigger_event_type == "power_on":
                trigger_event_type_name = "電源ON"
                trigger_event_detail_name = (
                    "正常"
                    if remote_control.get("automation_trigger_event_detail_flag") == "0"
                    else "異常"
                )
            trigger_device_name = (
                remote_control.get("automation_trigger_device_name")
                if remote_control.get("automation_trigger_device_name")
                else f"【{device.get("device_data", {}).get("param", {}).get("device_code")}】{remote_control.get("automation_trigger_imei")}(IMEI)"
            )
            event_detail = f"""
                　【オートメーション(失敗)】
                　{do_name}コントロールコマンドがデバイスに届きましたが、{di_name}が変化しませんでした。
                　 ※オートメーション「{trigger_device_name} ／ {trigger_event_type_name} ／ {trigger_event_detail_name}」
            """

    mail_subject = "イベントが発生しました"
    event_detail = textwrap.dedent(event_detail)
    mail_body = textwrap.dedent(
        f"""\
        ■発生日時：{send_datetime.strftime('%Y/%m/%d %H:%M:%S')}

        ■グループ：{group_name}
        　デバイス：{device_name}

        ■イベント内容
    """
    ).strip()
    mail_body = mail_body + event_detail
    mail.send_email(mail_to_list, mail_subject, textwrap.dedent(mail_body))

    # 通知履歴登録
    notification_hist_id = ddb.put_notification_hist(
        remote_control.get("contract_id"),
        device.get("device_data", {}).get("config", {}).get("notification_target_list", []),
        send_datetime,
        notification_hist_table,
    )

    return notification_hist_id


def lambda_handler(event, context):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            remote_controls_table = dynamodb.Table(ssm.table_names["REMOTE_CONTROL_TABLE"])
            hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            notification_hist_table = dynamodb.Table(ssm.table_names["NOTIFICATION_HIST_TABLE"])
            device_state_table = dynamodb.Table(ssm.table_names["STATE_TABLE"])
        except KeyError as e:
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(
            event,
            contract_table,
            device_relation_table,
            remote_controls_table,
        )
        if validate_result["code"] != "0000":
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        remote_control = validate_result["remote_control"]

        link_di_no = remote_control["link_di_no"]

        req_datetime = remote_control["req_datetime"]
        limit_datetime = req_datetime + 10000  # 10秒
        if time.time() <= limit_datetime / 1000:
            # タイムアウト時間まで待機
            time.sleep(float(limit_datetime) / 1000 - time.time())

        remote_control = ddb.get_remote_control_info(
            remote_control["device_req_no"], remote_controls_table
        )
        if not remote_control:
            body = {"code": "9999", "message": "端末要求番号が存在しません。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }


        device = db.get_device_info_other_than_unavailable(remote_control.get("device_id"), device_table)
        if not device:
            body = {"code": "9999", "message": "デバイス情報が存在しません。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        if remote_control.get("control_result") is None:
            # タイムアウト発生
            notification_setting = [
                setting
                for setting in device.get("device_data", {})
                .get("config", {})
                .get("notification_settings", [])
                if setting.get("event_trigger") == "do_change"
                and setting.get("terminal_no") == remote_control["do_no"]
            ]

            notification_hist_id = None
            if notification_setting:
                notification_hist_id = send_mail(
                    notification_setting[0],
                    device,
                    remote_control,
                    account_table,
                    user_table,
                    group_table,
                    device_relation_table,
                    notification_hist_table,
                    change_state_mail=False,
                )

            # 履歴レコード作成
            ddb.put_hist_list(
                remote_control,
                notification_hist_id,
                "timeout_response",
                hist_list_table,
                device_table,
                group_table,
                device_relation_table,
                device_state_table,
            )

            # 接点衆力制御応答テーブルに制御結果（タイムアウト）を登録
            ddb.update_remote_control_result_timeout(
                remote_control.get("device_req_no"),
                remote_control.get("req_datetime"),
                link_di_no,
                remote_controls_table,
            )

            return

        if link_di_no > 0:
            # 接点入力紐づけ設定あり
            recv_datetime = remote_control["recv_datetime"]
            limit_datetime = recv_datetime + 20000  # 20秒
            if time.time() <= limit_datetime / 1000:
                # タイムアウト時間まで待機
                time.sleep(float(limit_datetime) / 1000 - time.time())

            remote_control = ddb.get_remote_control_info(
                remote_control["device_req_no"], remote_controls_table
            )
            if remote_control.get("link_di_result") != "0":
                notification_setting = [
                    setting
                    for setting in device.get("device_data", {})
                    .get("config", {})
                    .get("notification_settings", [])
                    if setting.get("event_trigger") == "do_change"
                    and setting.get("terminal_no") == remote_control["do_no"]
                ]

                notification_hist_id = None
                if notification_setting:
                    notification_hist_id = send_mail(
                        notification_setting[0],
                        device,
                        remote_control,
                        account_table,
                        user_table,
                        group_table,
                        device_relation_table,
                        notification_hist_table,
                        change_state_mail=True,
                    )

                # 履歴レコード作成
                ddb.put_hist_list(
                    remote_control,
                    notification_hist_id,
                    "timeout_status",
                    hist_list_table,
                    device_table,
                    group_table,
                    device_relation_table,
                    device_state_table,
                )

                # 接点衆力制御応答テーブルに接点入力状態変化の結果（タイムアウト）を登録
                ddb.update_link_di_result_timeout(
                    remote_control.get("device_req_no"),
                    remote_control.get("req_datetime"),
                    remote_controls_table,
                )

    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        raise Exception(json.dumps(res_body))
