import json
import os
import re
import textwrap
import time
import traceback
import uuid
from datetime import datetime
from zoneinfo import ZoneInfo
from dateutil import relativedelta

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import auth
import convert
import db
import ddb
import mail
import ssm
import validate

patch_all()

logger = Logger()

# 環境変数
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
LAMBDA_TIMEOUT_CHECK = os.environ["LAMBDA_TIMEOUT_CHECK"]
REMOTE_CONTROLS_TTL = int(os.environ["REMOTE_CONTROLS_TTL"])
CNT_HIST_TTL = int(os.environ["CNT_HIST_TTL"])

# レスポンスヘッダー
res_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}

# AWSリソース定義
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_DEFAULT_REGION,
    endpoint_url=os.environ.get("endpoint_url"),
)
iot = boto3.client("iot-data", region_name=AWS_DEFAULT_REGION)
aws_lambda = boto3.client("lambda", region_name=AWS_DEFAULT_REGION)


@auth.verify_login_user()
def lambda_handler(event, context, user_info):
    try:
        ### 0. DynamoDBの操作オブジェクト生成
        try:
            account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            req_no_counter_table = dynamodb.Table(ssm.table_names["REQ_NO_COUNTER_TABLE"])
            control_status_table = dynamodb.Table(ssm.table_names["CONTROL_STATUS_TABLE"])
            remote_controls_table = dynamodb.Table(ssm.table_names["REMOTE_CONTROL_TABLE"])
            hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            notification_hist_table = dynamodb.Table(ssm.table_names["NOTIFICATION_HIST_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        ### 1. 入力情報チェック
        # 入力情報のバリデーションチェック
        val_result = validate.validate(event, user_info, account_table)
        if val_result.get("message"):
            logger.info("Error in validation check of input information.")
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(val_result, ensure_ascii=False),
            }
        user_name = val_result["account_info"]["user_data"]["config"]["user_name"]
        email_address = val_result["account_info"]["email_address"]
        device_id = val_result["path_params"]["device_id"]
        do_no = int(val_result["path_params"]["do_no"])
        do_control = val_result["body"]["do_control"]
        do_specified_time = val_result["body"]["do_specified_time"]
        do_di_return = val_result["body"]["do_di_return"]

        ### 2. デバイス捜査権限チェック（共通）
        # デバイスID一覧取得
        contract_id = user_info["contract_id"]
        contract_info = db.get_contract_info(contract_id, contract_table)
        if not contract_info:
            res_body = {"message": "契約情報が存在しません。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.info(f"contract_info: {contract_info}")
        device_list = contract_info["contract_data"]["device_list"]

        # デバイス操作権限チェック
        if device_id not in device_list:
            res_body = {
                "message": "権限が変更されたデバイスが選択されました。\n画面の更新を行います。\n\nエラーコード：003-0607"
            }
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 3. デバイス捜査権限チェック（作業者の場合）
        # デバイス操作権限チェック
        if user_info["user_type"] == "worker":
            device_id_list = db.get_user_relation_device_id_list(
                user_info["user_id"], device_relation_table
            )
            logger.info(f"device_id_list: {device_id_list}")

            if device_id not in device_id_list:
                res_body = {
                    "message": "権限が変更されたデバイスが選択されました。\n画面の更新を行います。\n\nエラーコード：003-0607"
                }
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
        else:
            pass

        ### 4. 制御情報取得
        device_info = ddb.get_device_info_other_than_unavailable(device_id, device_table)
        for do_item in device_info[0].get("device_data").get("config").get("terminal_settings").get("do_list"):
            if do_item.get("do_no") == do_no:
                if do_item.get("do_flag") == 0:
                    res_body = {"message": "制御不可の端子に対する操作はできません"}
                    return {
                        "statusCode": 400,
                        "headers": res_headers,
                        "body": json.dumps(res_body, ensure_ascii=False),
                    }
                if not (do_item.get("do_control") == do_control and
                        do_item.get("do_specified_time") == do_specified_time and
                        do_item.get("do_di_return") == do_di_return):
                    res_body = {
                        "message": "コントロール設定が変更されたため、実施しませんでした。",
                        "error_flag": 1
                    }
                    return {
                        "statusCode": 400,
                        "headers": res_headers,
                        "body": json.dumps(res_body, ensure_ascii=False),
                    }
        if len(device_info) == 0:
            res_body = {"message": "デバイス情報が存在しません。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        device_info = device_info[0]
        if device_info["device_type"] != "PJ2":
            res_body = {"message": "デバイス種別が想定と一致しません。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.info(f"device_info: {device_info}")

        ### 5. 制御中判定
        # 制御状況を追加（同時処理の排他制御）
        do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
        do_info = [do for do in do_list if int(do["do_no"]) == do_no][0]
        if ("do_di_return" in do_info) and do_info["do_di_return"]:
            # 紐づけありの場合は、30秒後に制御状況を自動削除
            delete_second = 30
        else:
            # 紐づけなしの場合は、10秒後に制御状況を自動削除
            delete_second = 10
        if not ddb.check_control_status(device_id, do_no, delete_second, control_status_table):
            res_body = {"message": "他のユーザー操作、タイマーまたは連動により制御中です。"}
            logger.info(res_body)
            return {
                "statusCode": 409,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        # 最新制御情報を確認
        remote_control_latest = ddb.get_remote_control_latest(
            device_info["device_id"], do_no, remote_controls_table
        )
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
                logger.info(
                    "Not processed because it was judged that it was already under control"
                )
                regist_result = __register_hist_info(
                    device_info,
                    do_no,
                    remote_control_latest.get("control_trigger"),
                    user_name,
                    email_address,
                    user_table,
                    account_table,
                    group_table,
                    device_relation_table,
                    notification_hist_table,
                    hist_list_table,
                    remote_control_latest.get("req_datetime")
                )
                # 制御状況を削除
                control_status_table.delete_item(
                    Key={"device_id": device_id, "do_no": do_no},
                )
                if not regist_result[0]:
                    return {
                        "statusCode": 500,
                        "headers": res_headers,
                        "body": json.dumps(regist_result[1], ensure_ascii=False),
                    }
                if remote_control_latest.get("control_trigger") == "manual_control":
                    error_flag = 4
                elif remote_control_latest.get("control_trigger") in ["timer_control", "on_timer_control", "off_timer_control"]:
                    error_flag = 2
                else:
                    error_flag = 3
                res_body = {
                    "message": "他のユーザー操作、タイマーまたは連動により制御中です。",
                    "error_flag": error_flag
                }
                return {
                    "statusCode": 409,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }

        # 要求番号取得
        icc_id = device_info["device_data"]["param"]["iccid"]
        req_no_count_info = ddb.get_req_no_count_info(icc_id, req_no_counter_table)
        if req_no_count_info:
            logger.info(f"req_no_count_info: {req_no_count_info}")

            ### 6. 要求番号生成（アトミックカウンタをインクリメントし、端末要求番号を生成）
            req_num = ddb.increment_req_no_count_num(icc_id, req_no_counter_table)

        else:
            ### 6. 要求番号生成（カウント0 のレコード作成し、カウント0 の端末要求番号を生成）
            logger.info("req_no_count_info did not exist. Put req_no_count_info to table")
            req_num = 0
            write_items = [
                {
                    "Put": {
                        "TableName": ssm.table_names["REQ_NO_COUNTER_TABLE"],
                        "Item": {"simid": {"S": icc_id}, "num": {"N": str(req_num)}},
                    }
                }
            ]
            result = db.execute_transact_write_item(write_items)
            if not result:
                # 制御状況を削除
                control_status_table.delete_item(
                    Key={"device_id": device_id, "do_no": do_no},
                )
                res_body = {"message": "要求番号カウンタ情報への書き込みに失敗しました。"}
                return {
                    "statusCode": 500,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
        req_no = re.sub("^0x", "", format(req_num % 65535, "#010x"))

        ### 7. 接点出力制御要求
        # 接点出力制御要求メッセージを生成
        topic = "cmd/" + icc_id
        # 接点出力_制御状態・接点出力_制御時間を判定
        do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
        do_info = [do for do in do_list if int(do["do_no"]) == do_no][0]
        if do_info["do_control"] == "open":
            do_specified_time = float(do_info["do_specified_time"])
            do_control = "01"
            # 制御時間は 0.1 秒を 1 として16進数4バイトの値を設定
            do_control_time = re.sub("^0x", "", format(int(do_specified_time * 10), "#06x"))
        elif do_info["do_control"] == "close":
            do_specified_time = float(do_info["do_specified_time"])
            do_control = "00"
            # 制御時間は 0.1 秒を 1 として16進数4バイトの値を設定
            do_control_time = re.sub("^0x", "", format(int(do_specified_time * 10), "#06x"))
        elif do_info["do_control"] == "toggle":
            do_control = "10"
            do_control_time = "0000"
        else:
            # 制御状況を削除
            control_status_table.delete_item(
                Key={"device_id": device_id, "do_no": do_no},
            )
            res_body = {"message": "接点出力_制御方法の値が不正です。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

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
        # pubhex = "000C80020000000001100000"
        logger.info(f"Iot Core Message(hexadecimal): {pubhex}")

        # AWS Iot Core へメッセージ送信
        iot_result = iot.publish(topic=topic, qos=0, retain=False, payload=bytes.fromhex(pubhex))
        logger.info(f"iot_result: {iot_result}")

        # 要求データを接点出力制御応答TBLへ登録
        device_req_no = icc_id + "-" + req_no
        do_di_return = convert.decimal_default_proc(do_info["do_di_return"])
        now_unixtime = int(time.time() * 1000)
        expire_datetime = int(
            (
                datetime.fromtimestamp(now_unixtime / 1000)
                + relativedelta.relativedelta(years=REMOTE_CONTROLS_TTL)
            ).timestamp()
        )
        put_items = [
            {
                "Put": {
                    "TableName": ssm.table_names["REMOTE_CONTROL_TABLE"],
                    "Item": {
                        "device_req_no": {"S": device_req_no},
                        "req_datetime": {"N": str(now_unixtime)},
                        "expire_datetime": {"N": str(expire_datetime)},
                        "device_id": {"S": device_id},
                        "contract_id": {"S": contract_id},
                        "control": {"S": do_info["do_control"]},
                        "control_trigger": {"S": "manual_control"},
                        "do_no": {"N": str(do_no)},
                        "link_di_no": {"N": str(do_di_return)},
                        "iccid": {"S": icc_id},
                        "control_exec_user_name": {"S": user_name},
                        "control_exec_user_email_address": {"S": email_address},
                    },
                }
            }
        ]
        result = db.execute_transact_write_item(put_items)
        if not result:
            # 制御状況を削除
            control_status_table.delete_item(
                Key={"device_id": device_id, "do_no": do_no},
            )
            res_body = {"message": "接点出力制御応答情報への書き込みに失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 8. タイムアウト判定Lambda呼び出し
        payload = {
            "headers": event["headers"],
            "body": json.dumps({"device_req_no": device_req_no}),
        }
        lambda_invoke_result = aws_lambda.invoke(
            FunctionName=LAMBDA_TIMEOUT_CHECK,
            InvocationType="Event",
            Payload=json.dumps(payload, ensure_ascii=False),
        )
        logger.info(f"lambda_invoke_result: {lambda_invoke_result}")

        # 制御状況を削除
        control_status_table.delete_item(
            Key={"device_id": device_id, "do_no": do_no},
        )

        ### 9. メッセージ応答
        res_body = {"message": "", "device_req_no": device_req_no}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }


def __register_hist_info(
    device_info,
    do_no,
    control_trigger,
    user_name,
    email_address,
    user_table,
    account_table,
    group_table,
    device_relation_table,
    notification_hist_table,
    hist_list_table,
    req_datetime
):
    """
    - 要求番号が設定されており、接点出力端子が制御中の場合
        履歴情報一覧へ実施しなかったことを登録する
    """
    result = None
    do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
    do_info = [do for do in do_list if int(do["do_no"]) == do_no][0]

    # グループ情報取得
    group_id_list = db.get_device_relation_group_id_list(
        device_info["device_id"], device_relation_table
    )
    group_list = list()
    for group_id in group_id_list:
        group_info = db.get_group_info(group_id, group_table)
        if not group_info:
            return False, {"message": "グループ情報が存在しません。"}
        logger.info(f"group_info: {group_info}")
        group_list.append(
            {
                "group_id": group_info["group_id"],
                "group_name": group_info["group_data"]["config"]["group_name"],
            }
        )

    # メール通知
    notification_setting = [
        setting
        for setting in device_info.get("device_data", {})
        .get("config", {})
        .get("notification_settings", [])
        if (setting.get("event_trigger") == "do_change")
        and (setting.get("terminal_no") == do_info["do_no"])
    ]
    notification_hist_id = ""
    if notification_setting:
        notification_hist_id = __send_mail(
            notification_setting[0],
            device_info,
            group_list,
            do_info,
            user_name,
            email_address,
            control_trigger,
            user_table,
            account_table,
            notification_hist_table,
            req_datetime
        )

    # 履歴情報登録
    now_unixtime = int(time.time() * 1000)
    expire_datetime = int(
        (
            datetime.fromtimestamp(now_unixtime / 1000)
            + relativedelta.relativedelta(years=REMOTE_CONTROLS_TTL)
        ).timestamp()
    )
    item = {
        "device_id": device_info["device_id"],
        "hist_id": str(uuid.uuid4()),
        "event_datetime": req_datetime,
        "expire_datetime": expire_datetime,
        "hist_data": {
            "device_name": device_info["device_data"]["config"]["device_name"],
            "group_list": group_list,
            "imei": device_info["imei"],
            "event_type": "manual_control",
            "terminal_no": int(do_info["do_no"]),
            "terminal_name": do_info["do_name"],
            "control_exec_user_name": user_name,
            "control_exec_user_email_address": email_address,
            "notification_hist_id": notification_hist_id,
            "control_result": "not_excuted_done",
            "not_excuted_done_reason": control_trigger,
        },
    }
    item = convert.dict_dynamo_format(item)
    put_items = [
        {
            "Put": {
                "TableName": hist_list_table.name,
                "Item": item,
            }
        }
    ]
    result = db.execute_transact_write_item(put_items)
    if not result:
        return False, {"message": "履歴一覧情報への書き込みに失敗しました。"}
    logger.info(f"put_items: {put_items}")

    return True, result


def __send_mail(
    notification_setting,
    device_info,
    group_list,
    do_info,
    user_name,
    email_address,
    control_trigger,
    user_table,
    account_table,
    notification_hist_table,
    req_datetime
):
    req_datetime_converted = datetime.fromtimestamp(req_datetime / 1000.0,ZoneInfo("Asia/Tokyo"))
    # メール送信内容の設定
    send_datetime = datetime.now(ZoneInfo("Asia/Tokyo"))

    device_config = device_info.get("device_data", {}).get("config", {})
    device_name = (
        device_config.get("device_name")
        if device_config.get("device_name")
        else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{device_info.get("imei")}(IMEI)"
    )
    group_name_list = [g["group_name"] for g in group_list]
    if group_name_list:
        group_name_list.sort()
    group_name = "、".join(group_name_list)

    # 接点出力名の設定
    do_name = do_info["do_name"]
    if not do_name:
        do_name = "接点出力" + str(do_info["do_no"])

    # ユーザー名の設定
    if not user_name:
        user_name = email_address

    # メール送信先の設定
    mail_to_list = []
    for user_id in device_config.get("notification_target_list", []):
        mail_user = db.get_user_info_by_user_id(user_id, user_table)
        mail_account = db.get_account_info_by_account_id(mail_user["account_id"], account_table)
        mail_to_list.append(mail_account.get("email_address"))
    logger.debug(f"mail_to_list: {mail_to_list}")

    if control_trigger == "manual_control":
        event_detail = f"""
            　【マニュアルコントロール(不実施)】
            　他のユーザー操作により、{do_name}をコントロール中だったため、コントロールを行いませんでした。
            　 ※{user_name}が操作
        """
    elif control_trigger in ["timer_control", "on_timer_control", "off_timer_control"]:
        event_detail = f"""
            　【マニュアルコントロール(不実施)】
            　スケジュールにより、{do_name}をコントロール中だったため、コントロールを行いませんでした。
            　 ※{user_name}が操作
        """
    else:
        event_detail = f"""
            　【マニュアルコントロール(不実施)】
            　オートメーションにより、{do_name}をコントロール中だったため、コントロールを行いませんでした。
            　 ※{user_name}が操作
        """
    event_detail = textwrap.dedent(event_detail)

    # メール送信
    mail_subject = "イベントが発生しました"
    mail_body = textwrap.dedent(
        f"""
        ■発生日時：{req_datetime_converted.strftime('%Y/%m/%d %H:%M:%S')}

        ■グループ：{group_name}
        　デバイス：{device_name}

        ■イベント内容
    """
    ).strip()
    mail_body = mail_body + event_detail
    logger.debug(f"mail_body: {mail_body}")
    mail.send_email(mail_to_list, mail_subject, textwrap.dedent(mail_body))

    # 通知履歴登録
    notification_hist_id = ddb.put_notification_hist(
        device_info["device_data"]["param"]["contract_id"],
        device_config.get("notification_target_list", []),
        send_datetime,
        notification_hist_table,
    )

    return notification_hist_id
