import os
import decimal
import re
import uuid
import json
import time
from datetime import datetime
from zoneinfo import ZoneInfo
import textwrap
from dateutil import relativedelta

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

import db
import ssm
import convert
import mail

logger = Logger()

AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
LAMBDA_TIMEOUT_CHECK = os.environ["LAMBDA_TIMEOUT_CHECK"]
HIST_LIST_TTL = int(os.environ["HIST_LIST_TTL"])
REMOTE_CONTROLS_TTL = int(os.environ["REMOTE_CONTROLS_TTL"])

aws_lambda = boto3.client("lambda", region_name=AWS_DEFAULT_REGION)
iot = boto3.client("iot-data", region_name=AWS_DEFAULT_REGION)
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


def decimal_to_num(obj):
    if isinstance(obj, decimal.Decimal):
        return int(obj) if float(obj).is_integer() else float(obj)


def automation_control(device_id, event_type, terminal_no, di_state, occurrence_flag):
    logger.info(
        f"exec automation control: {device_id} {event_type} {terminal_no} {di_state} {occurrence_flag}"
    )

    # パラメータチェック
    if not event_type:
        return {"result": False, "message": "イベント項目が指定されていません。"}
    if event_type == "di_change_state":
        if terminal_no is None:
            return {"result": False, "message": "接点端子が指定されていません。"}
        if di_state is None:
            return {"result": False, "message": "接点入力状態が指定されていません。"}
    elif event_type == "di_unhealthy":
        if terminal_no is None:
            return {"result": False, "message": "接点端子が指定されていません。"}
        if occurrence_flag is None:
            return {"result": False, "message": "発生フラグが指定されていません。"}
    else:
        if occurrence_flag is None:
            return {"result": False, "message": "発生フラグが指定されていません。"}

    # テーブル取得
    try:
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        device_state_table = dynamodb.Table(ssm.table_names["STATE_TABLE"])
        req_no_counter_table = dynamodb.Table(ssm.table_names["REQ_NO_COUNTER_TABLE"])
        remote_controls_table = dynamodb.Table(ssm.table_names["REMOTE_CONTROL_TABLE"])
        hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
        group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
        notification_hist_table = dynamodb.Table(ssm.table_names["NOTIFICATION_HIST_TABLE"])
        automation_table = dynamodb.Table(ssm.table_names["AUTOMATION_TABLE"])
        control_status_table = dynamodb.Table(ssm.table_names["CONTROL_STATUS_TABLE"])
    except KeyError as e:
        return {"result": False, "message": e}

    # 連動制御設定取得
    automation_list = _get_automation(
        automation_table, device_id, event_type, terminal_no, di_state, occurrence_flag
    )
    logger.info(f"automation setting: {automation_list}")
    if not automation_list:
        logger.info("automation setting not found")
        return {"result": False, "message": "連動制御設定が存在しません。"}

    # トリガーデバイス情報取得
    trigger_device = db.get_device_info_other_than_unavailable(device_id, device_table)

    result = True
    for automation in automation_list:
        control_trigger = ""
        logger.info(f"automation: {automation}")
        # 制御対象デバイス情報取得
        control_device = db.get_device_info_other_than_unavailable(
            automation["control_device_id"], device_table
        )

        # デバイス情報存在チェック
        if not trigger_device or not control_device:
            logger.info("device_info not found")
            result = False
            continue

        # 制御対象デバイスの接点出力設定取得
        control_device_do_list = (
            control_device.get("device_data", {})
            .get("config", {})
            .get("terminal_settings", {})
            .get("do_list", [])
        )
        control_do = [
            do for do in control_device_do_list if do["do_no"] == automation["control_do_no"]
        ][0]

        event_datetime = datetime.now()

        group_id_list = db.get_device_relation_group_id_list(
            control_device["device_id"], device_relation_table
        )
        group_list = []
        for group_id in group_id_list:
            group_info = db.get_group_info(group_id, group_table)
            if group_info:
                group_list.append(
                    {
                        "group_id": group_info["group_id"],
                        "group_name": group_info["group_data"]["config"]["group_name"],
                    }
                )
        if group_list:
            group_list = sorted(group_list, key=lambda x: x["group_name"])

        # 制御対象デバイスの紐づけ接点入力が指定されている場合、接点入力状態をチェック
        if control_do.get("do_di_return") and automation["control_di_state"] in [0, 1]:
            device_state = db.get_device_state(automation["control_device_id"], device_state_table)
            if not device_state:
                logger.info("制御対象デバイスの現状態情報が存在しません。")
                result = False
                continue

            col_name = "di" + str(control_do.get("do_di_return")) + "_state"
            if device_state[col_name] == automation["control_di_state"]:
                # 紐づき接点入力状態がすでに変更済みのため、制御不要
                # メール通知
                notification_hist_id = _send_not_exec_mail(
                    event_datetime,
                    trigger_device,
                    control_device,
                    group_list,
                    automation,
                    control_do,
                    account_table,
                    user_table,
                    notification_hist_table,
                    "link_di_state",
                    control_trigger,
                )

                # 履歴一覧登録
                _put_hist_list(
                    event_datetime,
                    trigger_device,
                    control_device,
                    group_list,
                    automation,
                    control_do,
                    notification_hist_id,
                    "not_excuted",
                    control_trigger,
                    hist_list_table,
                )

                logger.info("制御対象デバイスの接点入力状態がすでに変更済みです。")
                result = False
                continue

        # 制御中判定
        # 最新制御情報を確認
        remote_control_latest = _get_remote_control_latest(
            control_device.get("device_id"), control_do.get("do_no"), remote_controls_table
        )
        if remote_control_latest:
            control_trigger = remote_control_latest[0].get("control_trigger")
        else:
            control_trigger = ""
        # 制御状況を追加（同時処理の排他制御）
        if control_do.get("do_di_return"):
            # 紐づけありの場合は、30秒後に制御状況を自動削除
            delete_second = 30
        else:
            # 紐づけなしの場合は、10秒後に制御状況を自動削除
            delete_second = 10
        if not _check_control_status(
            control_device.get("device_id"),
            control_do.get("do_no"),
            delete_second,
            control_status_table,
        ):
            # メール通知
            notification_hist_id = _send_not_exec_mail(
                event_datetime,
                trigger_device,
                control_device,
                group_list,
                automation,
                control_do,
                account_table,
                user_table,
                notification_hist_table,
                "control_status",
                control_trigger,
            )

            # 履歴一覧登録
            _put_hist_list(
                event_datetime,
                trigger_device,
                control_device,
                group_list,
                automation,
                control_do,
                notification_hist_id,
                "not_excuted_done",
                control_trigger,
                hist_list_table,
            )

            logger.info("他のユーザー操作、タイマーまたは連動により制御中です。")
            result = False
            continue

        if len(remote_control_latest) > 0:
            remote_control_latest = remote_control_latest[0]
            link_di_no = remote_control_latest.get("link_di_no")
            logger.info(f"remote_control_latest: {remote_control_latest}")

            # 制御中判定
            if not remote_control_latest.get("control_result") or (
                link_di_no
                and remote_control_latest.get("control_result") != "9999"
                and not remote_control_latest.get("link_di_result")
            ):
                logger.info("Not processed because it was judged that it was already under control")
                # メール通知
                notification_hist_id = _send_not_exec_mail(
                    event_datetime,
                    trigger_device,
                    control_device,
                    group_list,
                    automation,
                    control_do,
                    account_table,
                    user_table,
                    notification_hist_table,
                    "control_status",
                    control_trigger,
                )

                # 履歴一覧登録
                _put_hist_list(
                    event_datetime,
                    trigger_device,
                    control_device,
                    group_list,
                    automation,
                    control_do,
                    notification_hist_id,
                    "not_excuted_done",
                    control_trigger,
                    hist_list_table,
                )

                # 制御状況を削除
                control_status_table.delete_item(
                    Key={
                        "device_id": control_device.get("device_id"),
                        "do_no": control_do.get("do_no"),
                    },
                )

                logger.info("他のユーザー操作、タイマーまたは連動により制御中です。")
                result = False
                continue

        # 要求番号生成
        icc_id = control_device["device_data"]["param"]["iccid"]
        req_no = _get_req_no(icc_id, req_no_counter_table)

        # 制御実行（MQTT）
        _cmd_exec(icc_id, req_no, control_do)

        # 要求データ登録
        device_req_no = icc_id + "-" + req_no
        _put_remote_controls(
            trigger_device,
            control_device,
            automation,
            device_req_no,
            control_do,
            remote_controls_table,
        )

        # タイムアウト判定Lambda呼び出し
        payload = {"body": json.dumps({"device_req_no": device_req_no})}
        lambda_invoke_result = aws_lambda.invoke(
            FunctionName=LAMBDA_TIMEOUT_CHECK,
            InvocationType="Event",
            Payload=json.dumps(payload, ensure_ascii=False),
        )
        logger.info(f"lambda_invoke_result: {lambda_invoke_result}")

        # 制御状況を削除
        control_status_table.delete_item(
            Key={"device_id": control_device.get("device_id"), "do_no": control_do.get("do_no")},
        )

    return {"result": result, "message": "連動制御を実行しました。"}


def _put_remote_controls(
    trigger_device,
    control_device,
    automation,
    device_req_no,
    control_do,
    remote_controls_table,
):
    now_unixtime = int(time.time() * 1000)
    expire_datetime = int(
        (
            datetime.fromtimestamp(now_unixtime / 1000)
            + relativedelta.relativedelta(years=REMOTE_CONTROLS_TTL)
        ).timestamp()
    )
    control_trigger = ""
    if automation.get("control_di_state") == 0:
        control_trigger = "on_automation_control"
    elif automation.get("control_di_state") == 1:
        control_trigger = "off_automation_control"
    elif automation.get("control_di_state") == 9:
        control_trigger = "automation_control"

    di_no = automation.get("trigger_terminal_no")
    di = [
        di
        for di in trigger_device.get("device_data", {})
        .get("config", {})
        .get("terminal_settings", {})
        .get("di_list", [])
        if di.get("di_no") == di_no
    ]
    trigger_di_name = (
        di[0].get("di_name") if di and di[0].get("di_name") else f"接点入力{di_no}"
    )

    trigger_di_state_name = ""
    if automation.get("trigger_event_detail_state") == 0:
        trigger_di_state_name = di[0].get("di_on_name", "クローズ")
    elif automation.get("trigger_event_detail_state") == 1:
        trigger_di_state_name = di[0].get("di_off_name", "オープン")

    remote_controls_item = {
        "device_req_no": device_req_no,
        "req_datetime": now_unixtime,
        "expire_datetime": expire_datetime,
        "device_id": control_device.get("device_id"),
        "contract_id": control_device.get("device_data", {}).get("param", {}).get("contract_id"),
        "control": control_do.get("do_control"),
        "control_trigger": control_trigger,
        "do_no": control_do.get("do_no"),
        "link_di_no": control_do.get("do_di_return"),
        "iccid": control_device.get("device_data", {}).get("param", {}).get("iccid"),
        "automation_trigger_device_name": trigger_device.get("device_data", {})
        .get("config", {})
        .get("device_name"),
        "automation_trigger_imei": trigger_device.get("imei"),
        "automation_trigger_event_type": automation.get("trigger_event_type"),
        "automation_trigger_terminal_no": di_no,
        "automation_trigger_terminal_name": trigger_di_name,
        "automation_trigger_terminal_state_name": trigger_di_state_name,
        "automation_trigger_event_detail_state": automation.get("trigger_event_detail_state"),
        "automation_trigger_event_detail_flag": automation.get("trigger_event_detail_flag"),
    }
    remote_controls_table.put_item(Item=remote_controls_item)


def _check_control_status(device_id, do_no, delete_second, table):
    try:
        put_item = {
            "device_id": device_id,
            "do_no": do_no,
            "del_datetime": int(time.time() + delete_second),
        }
        put_item_fmt = json.loads(
            json.dumps(put_item, default=decimal_to_num), parse_float=decimal.Decimal
        )
        table.put_item(
            Item=put_item_fmt,
            ConditionExpression="attribute_not_exists(device_id)",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info(e)
            return False
        else:
            raise


def _get_remote_control_latest(device_id, do_no, table):
    response = table.query(
        IndexName="device_id_req_datetime_index",
        KeyConditionExpression=Key("device_id").eq(device_id),
        FilterExpression=Attr("do_no").eq(do_no),
        ScanIndexForward=False,  # 降順
        Limit=1,
    ).get("Items", [])
    return response


def _put_hist_list(
    event_datetime,
    trigger_device,
    control_device,
    group_list,
    automation,
    control_do,
    notification_hist_id,
    control_result,
    control_trigger,
    hist_list_table,
):
    event_datetime_ms = int(time.mktime(event_datetime.timetuple()) * 1000) + int(
        event_datetime.microsecond / 1000
    )
    expire_datetime = int(
        (event_datetime + relativedelta.relativedelta(years=HIST_LIST_TTL)).timestamp()
    )

    di_name = None
    di_state_name = None
    di_no = control_do.get("do_di_return")
    if di_no:
        di = [
            di
            for di in control_device.get("device_data", {})
            .get("config", {})
            .get("terminal_settings", {})
            .get("di_list", [])
            if di.get("di_no") == di_no
        ]
        di_name = di[0].get("di_name") if di and di[0].get("di_name") else None
        if not di_name:
            di_name = f"接点入力{di_no}"

        if automation["control_di_state"] == 0:
            di_state_name = di[0].get("di_on_name", "クローズ")
        elif automation["control_di_state"] == 1:
            di_state_name = di[0].get("di_off_name", "オープン")

    event_type = ""
    if automation.get("control_di_state") == 0:
        event_type = "on_automation_control"
    elif automation.get("control_di_state") == 1:
        event_type = "off_automation_control"
    elif automation.get("control_di_state") == 9:
        event_type = "automation_control"

    di_no = automation.get("trigger_terminal_no")
    di = [
        di
        for di in trigger_device.get("device_data", {})
        .get("config", {})
        .get("terminal_settings", {})
        .get("di_list", [])
        if di.get("di_no") == di_no
    ]
    trigger_di_name = (
        di[0].get("di_name") if di and di[0].get("di_name") else f"接点入力{di_no}"
    )

    trigger_di_state_name = ""
    if automation.get("trigger_event_detail_state") == 0:
        trigger_di_state_name = di[0].get("di_on_name", "クローズ")
    elif automation.get("trigger_event_detail_state") == 1:
        trigger_di_state_name = di[0].get("di_off_name", "オープン")

    hist_list_item = {
        "device_id": control_device.get("device_id"),
        "hist_id": str(uuid.uuid4()),
        "event_datetime": event_datetime_ms,
        "expire_datetime": expire_datetime,
        "hist_data": {
            "device_name": control_device.get("device_data").get("config").get("device_name"),
            "imei": control_device.get("imei"),
            "group_list": group_list,
            "event_type": event_type,
            "terminal_no": control_do.get("do_no"),
            "terminal_name": control_do.get("do_name"),
            "link_terminal_no": control_do.get("do_di_return"),
            "link_terminal_name": di_name,
            "link_terminal_state_name": di_state_name,
            "automation_trigger_device_name": trigger_device.get("device_data", {})
            .get("config", {})
            .get("device_name"),
            "automation_trigger_imei": trigger_device.get("imei"),
            "automation_trigger_event_type": automation.get("trigger_event_type"),
            "automation_trigger_terminal_no": di_no,
            "automation_trigger_terminal_name": trigger_di_name,
            "automation_trigger_terminal_state_name": trigger_di_state_name,
            "automation_trigger_event_detail_state": automation.get("trigger_event_detail_state"),
            "automation_trigger_event_detail_flag": automation.get("trigger_event_detail_flag"),
            "notification_hist_id": notification_hist_id,
            "control_result": control_result,
        },
    }
    if control_result == "not_excuted_done":
        hist_list_item["hist_data"]["not_excuted_done_reason"] = control_trigger
    hist_list_table.put_item(Item=hist_list_item)


def _send_not_exec_mail(
    event_datetime,
    trigger_device,
    control_device,
    group_list,
    automation,
    control_do,
    account_table,
    user_table,
    notification_hist_table,
    mail_type,
    control_trigger,
):
    # 制御対象デバイスの通知設定を取得
    notification_setting = [
        setting
        for setting in control_device.get("device_data", {})
        .get("config", {})
        .get("notification_settings", [])
        if (setting.get("event_trigger") == "do_change")
        and (setting.get("terminal_no") == control_do.get("do_no"))
    ]
    if notification_setting:
        mail_to_list = []
        for user_id in (
            control_device.get("device_data", {})
            .get("config", {})
            .get("notification_target_list", [])
        ):
            mail_user = db.get_user_info_by_user_id(user_id, user_table)
            mail_account = db.get_account_info_by_account_id(
                mail_user["account_id"], account_table
            )
            mail_to_list.append(mail_account.get("email_address"))
        if mail_to_list:
            # メールに埋め込む情報を取得
            event_datetime_jst = event_datetime.astimezone(ZoneInfo("Asia/Tokyo"))

            trigger_device_name = (
                trigger_device.get("device_data", {}).get("config", {}).get("device_name")
                if trigger_device.get("device_data", {}).get("config", {}).get("device_name")
                else f"【{trigger_device.get("device_data", {}).get("param", {}).get("device_code")}】{trigger_device.get("imei")}(IMEI)"
            )

            control_device_name = (
                control_device.get("device_data", {}).get("config", {}).get("device_name")
                if control_device.get("device_data", {}).get("config", {}).get("device_name")
                else f"【{control_device.get("device_data", {}).get("param", {}).get("device_code")}】{control_device.get("imei")}(IMEI)"
            )

            group_name_list = []
            for group in group_list:
                group_name_list.append(group["group_name"])
            if group_name_list:
                group_name_list.sort()
            group_name = "、".join(group_name_list)

            do_name = (
                control_do.get("do_name")
                if control_do.get("do_name")
                else f"接点出力{control_do.get('do_no')}"
            )

            di_no = control_do.get("do_di_return")
            if di_no:
                di = [
                    di
                    for di in control_device.get("device_data", {})
                    .get("config", {})
                    .get("terminal_settings", {})
                    .get("di_list", [])
                    if di.get("di_no") == di_no
                ]
                di_name = (
                    di[0].get("di_name") if di and di[0].get("di_name") else f"接点入力{di_no}"
                )

                di_state_name = ""
                if automation["control_di_state"] == 0:
                    di_state_name = di[0].get("di_on_name", "クローズ")
                elif automation["control_di_state"] == 1:
                    di_state_name = di[0].get("di_off_name", "オープン")

            trigger_di_no = automation["trigger_terminal_no"]
            trigger_di = [
                trigger_di
                for trigger_di in trigger_device.get("device_data", {})
                .get("config", {})
                .get("terminal_settings", {})
                .get("di_list", [])
                if trigger_di.get("di_no") == trigger_di_no
            ]
            trigger_di_name = (
                trigger_di[0].get("di_name") if trigger_di and trigger_di[0].get("di_name") else f"接点入力{trigger_di_no}"
            )

            trigger_di_state_name = ""
            if automation["trigger_event_detail_state"] == 0:
                trigger_di_state_name = trigger_di[0].get("di_on_name", "クローズ")
            elif automation["trigger_event_detail_state"] == 1:
                trigger_di_state_name = trigger_di[0].get("di_off_name", "オープン")

            event_type_name = ""
            event_detail_name = ""
            if automation["trigger_event_type"] == "di_change_state":
                event_type_name = trigger_di_name if trigger_di_name else f"接点入力{trigger_di[0].get('di_no')}（{trigger_di_state_name}）"
                event_detail_name = trigger_di_state_name
            elif automation["trigger_event_type"] == "di_unhealthy":
                event_type_name = trigger_di_name if trigger_di_name else f"接点入力{trigger_di[0].get('di_no')}"
                if automation["trigger_event_detail_flag"] == 0:
                    event_detail_name = "接点入力検出復旧"
                elif automation["trigger_event_detail_flag"] == 1:
                    event_detail_name = "未変化検出"
            elif automation["trigger_event_type"] == "device_unhealthy":
                event_type_name = "デバイスヘルシー未受信"
                if automation["trigger_event_detail_flag"] == 0:
                    event_detail_name = "正常"
                elif automation["trigger_event_detail_flag"] == 1:
                    event_detail_name = "異常"
            elif automation["trigger_event_type"] == "battery_near":
                event_type_name = "バッテリー残量低下"
                if automation["trigger_event_detail_flag"] == 0:
                    event_detail_name = "正常"
                elif automation["trigger_event_detail_flag"] == 1:
                    event_detail_name = "異常"
            elif automation["trigger_event_type"] == "device_abnormality":
                event_type_name = "機器異常"
                if automation["trigger_event_detail_flag"] == 0:
                    event_detail_name = "正常"
                elif automation["trigger_event_detail_flag"] == 1:
                    event_detail_name = "異常"
            elif automation["trigger_event_type"] == "parameter_abnormality":
                event_type_name = "パラメータ異常"
                if automation["trigger_event_detail_flag"] == 0:
                    event_detail_name = "正常"
                elif automation["trigger_event_detail_flag"] == 1:
                    event_detail_name = "異常"
            elif automation["trigger_event_type"] == "fw_update_abnormality":
                event_type_name = "ファームウェア更新異常"
                if automation["trigger_event_detail_flag"] == 0:
                    event_detail_name = "正常"
                elif automation["trigger_event_detail_flag"] == 1:
                    event_detail_name = "異常"
            elif automation["trigger_event_type"] == "power_on":
                event_type_name = "電源ON"
                if automation["trigger_event_detail_flag"] == 0:
                    event_detail_name = "正常"
                elif automation["trigger_event_detail_flag"] == 1:
                    event_detail_name = "異常"

            # メール本文
            mail_body = ""
            if mail_type == "link_di_state":
                mail_body = textwrap.dedent(
                    f"""\
                    ■発生日時：{event_datetime_jst.strftime('%Y/%m/%d %H:%M:%S')}

                    ■グループ：{group_name}
                    　デバイス：{control_device_name}

                    ■イベント内容
                    　【オートメーション(不実施)】
                    　{di_name}がすでに{di_state_name}のため、{do_name}のコントロールを行いませんでした。
                    　 ※オートメーション「{trigger_device_name} ／ {event_type_name} ／ {event_detail_name}」
                """
                ).strip()
            elif mail_type == "control_status":
                if control_trigger == "manual_control":
                    mail_body = textwrap.dedent(
                        f"""\
                        ■発生日時：{event_datetime_jst.strftime('%Y/%m/%d %H:%M:%S')}

                        ■グループ：{group_name}
                        　デバイス：{control_device_name}

                        ■イベント内容
                        　【オートメーション(不実施)】
                        　他のユーザー操作により、{do_name}をコントロール中でした。
                        　そのため、コントロールを行いませんでした。
                        　 ※オートメーション「{trigger_device_name} ／ {event_type_name} ／ {event_detail_name}」
                    """
                    ).strip()
                elif control_trigger in ["timer_control", "on_timer_control", "off_timer_control"]:
                    mail_body = textwrap.dedent(
                        f"""\
                        ■発生日時：{event_datetime_jst.strftime('%Y/%m/%d %H:%M:%S')}

                        ■グループ：{group_name}
                        　デバイス：{control_device_name}

                        ■イベント内容
                        　【オートメーション(不実施)】
                        　スケジュールにより、{do_name}をコントロール中でした。
                        　そのため、コントロールを行いませんでした。
                        　 ※オートメーション「{trigger_device_name} ／ {event_type_name} ／ {event_detail_name}」
                    """
                    ).strip()
                else:
                    mail_body = textwrap.dedent(
                        f"""\
                        ■発生日時：{event_datetime_jst.strftime('%Y/%m/%d %H:%M:%S')}

                        ■グループ：{group_name}
                        　デバイス：{control_device_name}

                        ■イベント内容
                        　【オートメーション(不実施)】
                        　オートメーションにより、{do_name}をコントロール中でした。
                        　そのため、コントロールを行いませんでした。
                        　 ※オートメーション「{trigger_device_name} ／ {event_type_name} ／ {event_detail_name}」
                    """
                    ).strip()

            # メール送信
            mail.send_email(
                mail_to_list,
                "イベントが発生しました",
                mail_body,
            )

            # 通知履歴登録
            notification_hist_id = _put_notification_hist(
                trigger_device.get("device_data", {}).get("param", {}).get("contract_id"),
                control_device.get("device_data", {})
                .get("config", {})
                .get("notification_target_list", []),
                event_datetime,
                notification_hist_table,
            )
            return notification_hist_id


def _put_notification_hist(
    contract_id, notification_user_list, notification_datetime, notification_hist_table
):
    notification_hist_id = str(uuid.uuid4())
    notice_hist_item = {
        "notification_hist_id": notification_hist_id,
        "contract_id": contract_id,
        "notification_datetime": int(time.mktime(notification_datetime.timetuple()) * 1000)
        + int(notification_datetime.microsecond / 1000),
        "notification_user_list": notification_user_list,
    }
    item = json.loads(json.dumps(notice_hist_item), parse_float=decimal.Decimal)
    notification_hist_table.put_item(Item=item)
    return notification_hist_id


def _cmd_exec(icc_id, req_no, control_do):
    topic = "cmd/" + icc_id
    do_no = int(control_do["do_no"])

    if control_do["do_control"] == "open":
        do_specified_time = float(control_do["do_specified_time"])
        do_control = "01"
        # 制御時間は 0.1 秒を 1 として16進数4バイトの値を設定
        do_control_time = re.sub("^0x", "", format(int(do_specified_time * 10), "#06x"))
    elif control_do["do_control"] == "close":
        do_specified_time = float(control_do["do_specified_time"])
        do_control = "00"
        # 制御時間は 0.1 秒を 1 として16進数4バイトの値を設定
        do_control_time = re.sub("^0x", "", format(int(do_specified_time * 10), "#06x"))
    elif control_do["do_control"] == "toggle":
        do_control = "10"
        do_control_time = "0000"

    payload = {
        "Message_Length": "000C",
        "Message_type": "8002",
        "Req_No": req_no,
        "DO_No": format(do_no, "#02"),
        "DO_Control": do_control,
        "DO_ControlTime": do_control_time,
    }
    logger.info(f"Iot Core Message: {payload}")
    pubhex = "".join(payload.values())
    logger.info(f"Iot Core Message(hexadecimal): {pubhex}")

    topic = "cmd/" + icc_id
    iot_result = iot.publish(topic=topic, qos=0, retain=False, payload=bytes.fromhex(pubhex))
    logger.info(f"iot_result: {iot_result}")


def _get_req_no(sim_id, req_no_counter_table):
    req_no_count_info = req_no_counter_table.get_item(Key={"simid": sim_id}).get("Item", {})
    if req_no_count_info:
        # 要求番号生成（アトミックカウンタをインクリメントし、要求番号を取得）
        response = req_no_counter_table.update_item(
            Key={"simid": sim_id},
            UpdateExpression="ADD #key :increment",
            ExpressionAttributeNames={"#key": "num"},
            ExpressionAttributeValues={":increment": decimal.Decimal(1)},
            ReturnValues="UPDATED_NEW",
        )
        num = response.get("Attributes").get("num")
        count = int(convert.decimal_default_proc(num))

    else:
        count = 0
        write_items = [
            {
                "Put": {
                    "TableName": req_no_counter_table.name,
                    "Item": {"simid": {"S": sim_id}, "num": {"N": str(count)}},
                }
            }
        ]
        db.execute_transact_write_item(write_items)

    return re.sub("^0x", "", format(count % 65535, "#010x"))


def _get_automation(
    automation_table, device_id, event_type, terminal_no, di_state, occurrence_flag
):
    automation_list = automation_table.query(
        IndexName="trigger_device_id_index",
        KeyConditionExpression=Key("trigger_device_id").eq(device_id),
    ).get("Items")
    automation_list = [
        item
        for item in automation_list
        if item.get("trigger_event_type") == event_type
        and (
            (
                event_type == "di_change_state"
                and item.get("trigger_terminal_no") == terminal_no
                and item.get("trigger_event_detail_state") == di_state
            )
            or (
                event_type == "di_unhealthy"
                and item.get("trigger_terminal_no") == terminal_no
                and item.get("trigger_event_detail_flag") == occurrence_flag
            )
            or (
                event_type not in ["di_change_state", "di_unhealthy"]
                and item.get("trigger_event_detail_flag") == occurrence_flag
            )
        )
    ]
    return automation_list if automation_list else None
