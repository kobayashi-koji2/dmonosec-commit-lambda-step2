import json
import os
import traceback
from decimal import Decimal
from datetime import datetime
import time
import textwrap

from aws_lambda_powertools import Logger
import boto3
from botocore.exceptions import ClientError

import auth
import ssm
import db
import convert
import mail
import ddb
import validate

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]


def send_mail(
    user_info,
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
    send_datetime = datetime.now()
    device_name = (
        device.get("device_data", {}).get("config", {}).get("device_name", device.get("imei"))
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
    user_name = remote_control.get(
        "control_exec_user_name", remote_control.get("control_exec_user_email_address")
    )

    # メール送信
    mail_to_list = []
    for user_id in notification_setting.get("notification_target_list", []):
        logger.debug(user_id)
        mail_user = db.get_user_info_by_user_id(user_id, user_table)
        mail_account = db.get_account_info_by_account_id(mail_user["account_id"], account_table)
        mail_to_list.append(mail_account.get("email_address"))

    event_detail = ""
    if not change_state_mail:
        if remote_control.get("control_trigger") == "manual_control":
            event_detail = f"""\
                【画面操作による制御（失敗）】
                制御信号（{do_name}）がデバイスに届きませんでした。
                ※{user_name}が操作を行いました。
            """
        elif remote_control.get("control_trigger") == "on_timer_control":
            event_detail = f"""\
                【タイマーによる制御（失敗）】
                制御信号（{do_name}）がデバイスに届きませんでした。
                ※タイマー設定「ON制御 {remote_control.get("timer_time")}」により制御信号を送信しました。
            """
        elif remote_control.get("control_trigger") == "off_timer_control":
            event_detail = f"""\
                【タイマーによる制御（失敗）】
                制御信号（{do_name}）がデバイスに届きませんでした。
                ※タイマー設定「OFF制御 {remote_control.get("timer_time")}」により制御信号を送信しました。
            """
    else:
        di_no = remote_control.get("link_di_no")
        di = [
            di
            for di in device.get("config", {}).get("terminal_settings", {}).get("di_list", [])
            if di.get("di_no") == di_no
        ]
        di_name = di[0].get("di_name") if di and di[0].get("di_name") else f"接点入力{di_no}"
        if remote_control.get("control_trigger") == "manual_control":
            event_detail = f"""\
                【画面操作による制御（失敗）】
                制御信号（{do_name}）がデバイスに届きましたが、{di_name}が変化しませんでした。
                ※{user_name}が操作を行いました。
            """
        elif remote_control.get("control_trigger") == "on_timer_control":
            event_detail = f"""\
                【タイマーによる制御（失敗）】
                制御信号（{do_name}）がデバイスに届きましたが、{di_name}が変化しませんでした。
                ※タイマー設定「ON制御 {remote_control.get("timer_time")}」により制御信号を送信しました。
            """
        elif remote_control.get("control_trigger") == "off_timer_control":
            event_detail = f"""\
                【タイマーによる制御（失敗）】
                制御信号（{do_name}）がデバイスに届きましたが、{di_name}が変化しませんでした。
                ※タイマー設定「OFF制御 {remote_control.get("timer_time")}」により制御信号を送信しました。
            """

    mail_subject = "イベントが発生しました"
    mail_body = f"""\
        ■発生日時：{send_datetime.strftime('%y/%m/%d %H:%M:%S')}

        ■グループ：{group_name}
        　デバイス：{device_name}

        ■イベント内容
        {event_detail}
    """
    mail.send_email(mail_to_list, mail_subject, textwrap.dedent(mail_body))

    # 通知履歴登録
    notification_hist_id = ddb.put_notification_hist(
        user_info.get("contract_id"),
        notification_setting.get("notification_target_list", []),
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
            cnt_hist_table = dynamodb.Table(ssm.table_names["CNT_HIST_TABLE"])
            hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            notification_hist_table = dynamodb.Table(ssm.table_names["NOTIFICATION_HIST_TABLE"])
        except KeyError as e:
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        try:
            user_info = auth.verify_user(event, user_table)
        except auth.AuthError as e:
            logger.info("ユーザー検証失敗", exc_info=True)
            return {
                "statusCode": e.code,
                "headers": res_headers,
                "body": json.dumps({"message": e.message}, ensure_ascii=False),
            }

        logger.info(user_info)

        # パラメータチェック
        validate_result = validate.validate(
            event,
            user_info,
            contract_table,
            device_relation_table,
            remote_controls_table,
        )
        if validate_result["code"] != "0000":
            raise Exception(json.dumps(validate_result))

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

        device = db.get_device_info(remote_control.get("device_id"), device_table)

        if remote_control.get("control_result") is None:
            # タイムアウト発生
            notification_setting = [
                setting
                for setting in device.get("device_data", {})
                .get("config", {})
                .get("notification_settings", [])
                if setting.get("event_trigger") == "do_change"
            ]

            notification_hist_id = None
            if notification_setting:
                notification_hist_id = send_mail(
                    user_info,
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
            )

            # 接点衆力制御応答テーブルに制御結果（タイムアウト）を登録
            ddb.update_remote_control_result(
                remote_control.get("device_req_no"),
                remote_control.get("req_datetime"),
                "9999",
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

            cnt_hist_list = ddb.get_cnt_hist(
                remote_control["iccid"], recv_datetime, limit_datetime, cnt_hist_table
            )
            if not [
                cnt_hist for cnt_hist in cnt_hist_list if cnt_hist.get("di_trigger") == link_di_no
            ]:
                notification_setting = [
                    setting
                    for setting in device.get("config", {}).get("notification_settings", [])
                    if setting.get("event_trigger") == "do_change"
                ]

                notification_hist_id = None
                if notification_setting:
                    notification_hist_id = send_mail(
                        user_info,
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
                )

                # 接点衆力制御応答テーブルに制御結果（タイムアウト）を登録
                ddb.update_remote_control_result(
                    remote_control.get("device_req_no"),
                    remote_control.get("req_datetime"),
                    "9999",
                    remote_controls_table,
                )

    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        raise Exception(json.dumps(res_body))
