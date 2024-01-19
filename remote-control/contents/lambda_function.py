import json
import os
import re
import textwrap
import time
import traceback
import uuid
from datetime import datetime

import boto3
from aws_lambda_powertools import Logger

# layer
import auth
import convert
import db
import ddb
import mail
import ssm
import validate

logger = Logger()

# 環境変数
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
LAMBDA_TIMEOUT_CHECK = os.environ["LAMBDA_TIMEOUT_CHECK"]

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


@auth.verify_login_user
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
        device_id = val_result["path_params"]["device_id"]
        if device_id not in device_list:
            res_body = {"message": "デバイスの操作権限がありません。"}
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
                res_body = {"message": "デバイスの操作権限がありません。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
        else:
            pass

        ### 4. 制御情報取得
        device_info = ddb.get_device_info_other_than_unavailable(device_id, device_table)
        if len(device_info) == 0:
            res_body = {"message": "デバイス情報が存在しません。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        device_info = device_info[0]
        logger.info(f"device_info: {device_info}")

        ### 5. 制御中判定
        # 最新制御情報取得
        do_no = int(val_result["path_params"]["do_no"])
        remote_control_latest = ddb.get_remote_control_latest(
            device_info["device_id"], do_no, remote_controls_table
        )
        if len(remote_control_latest) > 0:
            remote_control_latest = remote_control_latest[0]
            logger.info(f"remote_control_latest: {remote_control_latest}")

            # 制御中判定
            if ("recv_datetime" not in remote_control_latest) or (
                remote_control_latest["recv_datetime"] == 0
            ):
                logger.info(
                    "Not processed because recv_datetime exists in remote_control_latest (judged as under control)"
                )
                regist_result = __register_hist_info(
                    device_info,
                    do_no,
                    user_name,
                    email_address,
                    user_table,
                    account_table,
                    group_table,
                    device_relation_table,
                    notification_hist_table,
                    hist_list_table,
                )
                if not regist_result[0]:
                    return {
                        "statusCode": 500,
                        "headers": res_headers,
                        "body": json.dumps(regist_result[1], ensure_ascii=False),
                    }
                res_body = {"message": "他のユーザー操作、タイマーまたは連動により制御中です。"}
                return {
                    "statusCode": 200,
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
            do_control = "00"
            do_specified_time = convert.decimal_default_proc(do_info["do_specified_time"])
            do_control_time = re.sub("^0x", "", format(do_specified_time, "#06x"))
        elif do_info["do_control"] == "close":
            do_control = "01"
            do_specified_time = convert.decimal_default_proc(do_info["do_specified_time"])
            do_control_time = re.sub("^0x", "", format(do_specified_time, "#06x"))
        elif do_info["do_control"] == "toggle":
            do_control = "10"
            do_control_time = "0000"
        else:
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
        put_items = [
            {
                "Put": {
                    "TableName": ssm.table_names["REMOTE_CONTROL_TABLE"],
                    "Item": {
                        "device_req_no": {"S": device_req_no},
                        "req_datetime": {"N": str(int(time.time() * 1000))},
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
    user_name,
    email_address,
    user_table,
    account_table,
    group_table,
    device_relation_table,
    notification_hist_table,
    hist_list_table,
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
        if setting.get("event_trigger") == "do_change"
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
            user_table,
            account_table,
            notification_hist_table
        )

    # 履歴情報登録
    item = {
        "device_id": device_info["device_id"],
        "hist_id": str(uuid.uuid4()),
        "event_datetime": int(time.time() * 1000),
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
    user_table,
    account_table,
    notification_hist_table
):
    # メール送信内容の設定
    send_datetime = datetime.now()
    device_config = device_info.get("device_data", {}).get("config", {})
    device_name = device_config.get("device_name", device_info.get("imei"))
    group_name_list = [g["group_name"] for g in group_list]
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
    for user_id in notification_setting.get("notification_target_list", []):
        mail_user = db.get_user_info_by_user_id(user_id, user_table)
        mail_account = db.get_account_info_by_account_id(mail_user["account_id"], account_table)
        mail_to_list.append(mail_account.get("email_address"))
    logger.debug(f"mail_to_list: {mail_to_list}")

    event_detail = f"""\
        【画面制御による制御（不実施）】
        他のユーザー操作、タイマーまたは連動設定により、{do_name}を制御中でした。
        そのため、制御を行いませんでした。
        ※{user_name}が操作を行いました。
    """

    # メール送信
    mail_subject = "イベントが発生しました"
    mail_body = f"""\
        ■発生日時：{send_datetime.strftime('%y/%m/%d %H:%M:%S')}

        ■グループ：{group_name}
        　デバイス：{device_name}

        ■イベント内容
        {event_detail}
    """
    logger.debug(f"mail_body: {mail_body}")
    mail.send_email(mail_to_list, mail_subject, textwrap.dedent(mail_body))

    # 通知履歴登録
    notification_hist_id = ddb.put_notification_hist(
        device_info["device_data"]["param"]["contract_id"],
        notification_setting.get("notification_target_list", []),
        send_datetime,
        notification_hist_table,
    )

    return notification_hist_id
