import os
import json
import time
import traceback
import uuid
import re
import textwrap
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from dateutil import relativedelta

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
import boto3

# layer
import ssm
import db
import convert
import ddb
import mail

patch_all()

logger = Logger()

# 環境変数
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
LAMBDA_TIMEOUT_CHECK = os.environ["LAMBDA_TIMEOUT_CHECK"]
REMOTE_CONTROLS_TTL = int(os.environ["REMOTE_CONTROLS_TTL"])
CNT_HIST_TTL = int(os.environ["CNT_HIST_TTL"])
# 正常レスポンス内容
respons = {
    "statusCode": 200,
    "headers": {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    },
    "body": "",
}
# AWSリソース定義
dynamodb = boto3.resource(
    "dynamodb", region_name=AWS_DEFAULT_REGION, endpoint_url=os.environ.get("endpoint_url")
)
iot = boto3.client("iot-data", region_name=AWS_DEFAULT_REGION)
aws_lambda = boto3.client("lambda", region_name=AWS_DEFAULT_REGION)


def lambda_handler(event, context):
    try:
        ### 0. DynamoDBの操作オブジェクト生成
        try:
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            device_state_table = dynamodb.Table(ssm.table_names["STATE_TABLE"])
            req_no_counter_table = dynamodb.Table(ssm.table_names["REQ_NO_COUNTER_TABLE"])
            remote_controls_table = dynamodb.Table(ssm.table_names["REMOTE_CONTROL_TABLE"])
            hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            notification_hist_table = dynamodb.Table(ssm.table_names["NOTIFICATION_HIST_TABLE"])
        except KeyError as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return -1

        for record in event['Records']:
            payload = json.loads(record["body"])

            ### 1. スケジュール設定チェック

            # パラメータ取得
            event_datetime = payload.get("event_datetime")
            dt_now = datetime.fromtimestamp(event_datetime)
            if dt_now.tzname != "JST":
                dt_now = dt_now + timedelta(hours=+9)
            contract_id = payload.get("contract_id")
            logger.info("now_time: {0}".format(dt_now.strftime("%H:%M")))
            logger.info(f"contract_id: {contract_id}")

            # 実行対象のデバイス情報取得
            device_info_list = ddb.get_device_info_by_contract_id(contract_id, contract_table, device_table)
            logger.debug(f"device_info_list={device_info_list}")

            ### 2. 接点出力制御要求
            error_flg = False
            reason = ""
            for device_info in device_info_list:
                logger.info(f"--- device_info: {device_info}")
                device_id = device_info.get("device_id")
                contract_id = device_info.get("device_data").get("param").get("contract_id")
                icc_id = device_info.get("device_data").get("param").get("iccid")
                do_list = device_info.get("device_data").get("config").get("terminal_settings").get("do_list")
                di_list = device_info.get("device_data").get("config").get("terminal_settings").get("di_list")

                for do_info in do_list:
                    # タイマー設定チェック
                    checked_timer_do_info = __check_timer_settings(do_info, dt_now)
                    if not checked_timer_do_info:
                        logger.info(
                            f"[__check_timer_settings(): FALSE] device_id: {device_id}, do_info: {do_info}"
                        )
                        continue

                    # 接点入力状態チェック
                    error_flg, result = __check_return_di_state(
                        checked_timer_do_info, device_id, device_state_table
                    )
                    if not error_flg:
                        logger.info(
                            f"[__check_return_di_state(): FALSE] device_id: {device_id}, result: {result}"
                        )
                        continue
                    if result == 1:
                        error_flg, result = __register_hist_info(
                            "__check_return_di_state",
                            reason,
                            device_info,
                            do_info,
                            di_list,
                            group_table,
                            device_relation_table,
                            user_table,
                            account_table,
                            notification_hist_table,
                            hist_list_table,
                            remote_controls_table
                        )
                        logger.info(
                            f"[__check_return_di_state(): FALSE] device_id: {device_id}, do_info: {do_info}"
                        )
                        continue
                    elif not result:
                        logger.info(
                            f"[__check_return_di_state(): FALSE] device_id: {device_id}, do_info: {do_info}"
                        )
                        continue
                    checked_di_state_info = result

                    # 制御中判定
                    error_flg, result, reason = __check_under_control(
                        checked_di_state_info,
                        icc_id,
                        device_id,
                        req_no_counter_table,
                        remote_controls_table,
                    )
                    if not error_flg:
                        logger.info(
                            f"[__check_under_control(): FALSE] device_id: {device_id}, result: {result}"
                        )
                        continue
                    if result == 1:
                        error_flg, result = __register_hist_info(
                            "__check_under_control",
                            reason,
                            device_info,
                            do_info,
                            di_list,
                            group_table,
                            device_relation_table,
                            user_table,
                            account_table,
                            notification_hist_table,
                            hist_list_table,
                        )
                        if not error_flg:
                            logger.info(
                                f"[__check_under_control(): FALSE] device_id: {device_id}, result: {result}"
                            )
                            continue
                        logger.info(
                            f"[__check_under_control(): FALSE] device_id: {device_id}, do_info: {do_info}"
                        )
                        continue
                    elif not result:
                        logger.info(
                            f"[__check_under_control(): FALSE] device_id: {device_id}, do_info: {do_info}"
                        )
                        continue
                    checked_under_control_info = result

                    # 端末向け要求番号生成
                    req_no = re.sub(
                        "^0x", "", format(checked_under_control_info["req_num"] % 65535, "#010x")
                    )

                    # 接点出力制御要求メッセージを生成
                    topic = "cmd/" + icc_id
                    do_no = int(do_info["do_no"])

                    if do_info["do_control"] == "open":
                        do_specified_time = float(do_info["do_specified_time"])
                        do_control = "01"
                        # 制御時間は 0.1 秒を 1 として16進数4バイトの値を設定
                        do_control_time = re.sub(
                            "^0x", "", format(int(do_specified_time * 10), "#06x")
                        )
                    elif do_info["do_control"] == "close":
                        do_specified_time = float(do_info["do_specified_time"])
                        do_control = "00"
                        # 制御時間は 0.1 秒を 1 として16進数4バイトの値を設定
                        do_control_time = re.sub(
                            "^0x", "", format(int(do_specified_time * 10), "#06x")
                        )
                    elif do_info["do_control"] == "toggle":
                        do_control = "10"
                        do_control_time = "0000"
                    else:
                        logger.info(f"接点出力_制御方法の値が不正です。 device_id={device_id}")
                        continue

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

                    # AWS Iot Core へメッセージ送信
                    iot_result = iot.publish(
                        topic=topic, qos=0, retain=False, payload=bytes.fromhex(pubhex)
                    )
                    logger.info(f"iot_result: {iot_result}")

                    # 要求データを接点出力制御応答TBLへ登録
                    device_req_no = icc_id + "-" + req_no
                    do_di_return = int(do_info["do_di_return"])
                    do_onoff_control = int(do_info["do_timer"]["do_onoff_control"])

                    control_trigger_map = {
                        0: "on_timer_control",
                        1: "off_timer_control",
                        9: "timer_control"
                    }
                    control_trigger = control_trigger_map.get(do_onoff_control, None)

                    if do_di_return != 0:
                        link_terminal = next((di for di in di_list if di["di_no"] == do_di_return), None)
                        if do_onoff_control == 0:
                            link_terminal_state_name = link_terminal.get("di_on_name", "クローズ")
                        elif do_onoff_control == 1:
                            link_terminal_state_name = link_terminal.get("di_off_name", "オープン")
                        elif do_onoff_control == 9:
                            link_terminal_state_name = ""
                    else:
                        logger.info(f"接点出力_ON/OFF制御の値が不正です。 device_id={device_id}")
                        continue

                    now_unixtime = int(time.time() * 1000)
                    expire_datetime = int(
                        (
                            datetime.fromtimestamp(now_unixtime / 1000)
                            + relativedelta.relativedelta(years=REMOTE_CONTROLS_TTL)
                        ).timestamp()
                    )
                    do_timer_name = do_info["do_timer"]["do_timer_name"]
                    if not do_timer_name:
                        do_timer_name = "無題のスケジュール"
                    put_items = [
                        {
                            "Put": {
                                "TableName": remote_controls_table.name,
                                "Item": {
                                    "device_req_no": {"S": device_req_no},
                                    "req_datetime": {"N": str(now_unixtime)},
                                    "expire_datetime": {"N": str(expire_datetime)},
                                    "device_id": {"S": device_id},
                                    "contract_id": {"S": contract_id},
                                    "control": {"S": do_info["do_control"]},
                                    "control_trigger": {"S": control_trigger},
                                    "do_no": {"N": str(do_no)},
                                    "link_di_no": {"N": str(do_di_return)},
                                    "iccid": {"S": icc_id},
                                    "timer_time": {"S": do_info["do_timer"]["do_time"]},
                                    "do_timer_name": {"S": do_timer_name},
                                    "link_terminal_state_name": {"S": link_terminal_state_name},
                                },
                            }
                        }
                    ]
                    result = db.execute_transact_write_item(put_items)
                    if not result:
                        logger.info(f"接点出力制御応答情報への書き込みに失敗しました。 device_id={device_id}")
                        continue
                    logger.info(f"put_items: {put_items}")

                    # タイムアウト判定Lambda呼び出し
                    payload = {"body": json.dumps({"device_req_no": device_req_no})}
                    lambda_invoke_result = aws_lambda.invoke(
                        FunctionName=LAMBDA_TIMEOUT_CHECK,
                        InvocationType="Event",
                        Payload=json.dumps(payload, ensure_ascii=False),
                    )
                    logger.info(f"lambda_invoke_result: {lambda_invoke_result}")

        logger.debug("lambda_handler正常終了")
        return 0

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        raise Exception('lambda_handler異常終了')


def __check_timer_settings(do_info, dt_now):
    """
    - タイマー設定のある接点出力情報かどうかを確認する。
    """
    result = None
    s_format = "%H:%M"

    # タイマー設定のある接点出力情報かどうか
    if ("do_timer_list" in do_info) and (len(do_info["do_timer_list"]) != 0):
        # タイマー時刻と一致する接点出力タイマーを抽出
        for do_timer in do_info["do_timer_list"]:
            do_time = datetime.strptime(do_timer["do_time"], s_format)
            do_weekday = do_timer.get("do_weekday", "").split(",")
            # 日曜日を0、土曜日を6とした整数
            now_weekday = (dt_now.weekday() + 1) % 7
            if (
                (do_time.hour == dt_now.hour)
                and (do_time.minute == dt_now.minute)
                and str(now_weekday) in do_weekday
            ):
                do_info["do_timer"] = do_timer
                result = do_info
                # 同一接点出力端子で重複した設定は不可となるので１つだけ抽出とする
                break
    return result


def __check_return_di_state(do_info, device_id, device_state_table):
    """
    1. 紐づく接点入力端子番号の指定があり、ON/OFF制御の指定がある場合
        その出力端子に紐づく接点入力端子の現状態を確認し、タイマーのON_OFF制御と比較検証する。
            1. タイマーのON_OFF制御と紐づく接点入力端子の現状態の値が一致しない場合
                処理続行する。
            2. タイマーのON_OFF制御と紐づく接点入力端子の現状態の値が一致する場合
                履歴情報を登録して処理対象外としてスキップする。
    2. 紐づく接点入力端子番号の指定がない場合、もしくはON/OFF制御の指定がない場合
        処理対象として処理続行する。
    """
    result = None

    if (
        ("do_di_return" in do_info)
        and do_info["do_di_return"]
        and do_info["do_timer"]["do_onoff_control"] in [0, 1]
    ):
        device_state_info = db.get_device_state(device_id, device_state_table)
        if not device_state_info:
            res_body = {"message": "現状態情報が存在しません。"}
            return False, res_body
        logger.info(f"device_state_info: {device_state_info}")

        # タイマーのON/OFF制御と接点入力状態の値が一致しないなら処理続行
        col_name = "di" + str(do_info["do_di_return"]) + "_state"
        if do_info["do_timer"]["do_onoff_control"] != device_state_info[col_name]:
            result = do_info
        else:
            logger.info(
                f"Not processed because the values of do_onoff_control and {col_name} match"
            )
            result = 1
    else:
        result = do_info
        logger.info("Not processed because do_di_return is not set")
        pass

    return True, result


def __check_under_control(do_info, icc_id, device_id, req_no_counter_table, remote_controls_table):
    """
    1. 要求番号が設定されている場合
        最新の制御情報を確認し、接点出力端子が制御中なのかどうか判定する。
            1. 制御中以外の場合
                処理続行する。
            2. 制御中の場合
                履歴情報を登録して処理対象外としてスキップする。
    2. 要求番号が設定されていない場合
        要求番号テーブルへnum:0のレコードを作成する。
    """
    result = None
    reason = ""

    # 最新制御情報取得
    remote_control_latest = ddb.get_remote_control_latest(
        device_id, do_info["do_no"], remote_controls_table
    )
    logger.info(f"remote_control_list: {remote_control_latest}")
    if len(remote_control_latest) > 0:
        remote_control_latest = remote_control_latest[0]
        link_di_no = remote_control_latest.get("link_di_no")

        # 制御中判定
        if not remote_control_latest.get("control_result") or (
            link_di_no
            and remote_control_latest.get("control_result") != "9999"
            and not remote_control_latest.get("link_di_result")
        ):
            logger.info("Not processed because it was judged that it was already under control")
            reason = remote_control_latest.get("control_trigger")
            return True, 1, reason

    req_no_count_info = ddb.get_req_no_count_info(icc_id, req_no_counter_table)
    if req_no_count_info:
        # 要求番号生成（アトミックカウンタをインクリメントし、要求番号を取得）
        req_num = ddb.increment_req_no_count_num(icc_id, req_no_counter_table)
        result = do_info
        result["req_num"] = int(req_num)

    else:
        logger.info("req_no_count_info did not exist. Put req_no_count_info to table")
        req_num = 0
        write_items = [
            {
                "Put": {
                    "TableName": req_no_counter_table.name,
                    "Item": {"simid": {"S": icc_id}, "num": {"N": str(req_num)}},
                }
            }
        ]
        result = db.execute_transact_write_item(write_items)
        if not result:
            res_body = {"message": "要求番号カウンタ情報への書き込みに失敗しました。"}
            return False, res_body, reason
        result = do_info
        result["req_num"] = req_num

    return True, result, reason


def __register_hist_info(
    flg,
    reason,
    device_info,
    do_info,
    di_list,
    group_table,
    device_relation_table,
    user_table,
    account_table,
    notification_hist_table,
    hist_list_table,
    remote_controls_table
):
    """
    1. 紐づく接点入力端子番号の指定があり、その出力端子の現状態ステータスがタイマーのON_OFF制御の値と一致する場合
    2. 要求番号が設定されており、接点出力端子が制御中の場合

    - 履歴情報一覧へ実施しなかったことを登録する
    """
    result = None

    remote_control_latest = ddb.get_remote_control_latest(
        device_info.get("device_id"), do_info["do_no"], remote_controls_table
    )
    req_datetime = remote_control_latest.get("req_datetime")

    # グループ情報取得
    group_id_list = db.get_device_relation_group_id_list(
        device_info["device_id"], device_relation_table
    )
    group_list = list()
    for group_id in group_id_list:
        group_info = db.get_group_info(group_id, group_table)
        if not group_info:
            res_body = {"message": "グループ情報が存在しません。"}
            return False, res_body
        logger.info(f"group_info: {group_info}")
        group_list.append(
            {
                "group_id": group_info["group_id"],
                "group_name": group_info["group_data"]["config"]["group_name"],
            }
        )
    if group_list:
        group_list = sorted(group_list, key=lambda x: x["group_name"])

    # メール通知設定を取得
    notification_settings_list = (
        device_info.get("device_data", {}).get("config", {}).get("notification_settings", [])
    )
    notification_setting = [
        setting
        for setting in notification_settings_list
        if (setting.get("event_trigger") == "do_change")
        and (setting.get("terminal_no") == do_info["do_no"])
    ]

    # メール通知
    notification_hist_id = ""
    if notification_setting:
        notification_hist_id = __send_mail(
            flg,
            reason,
            notification_setting[0],
            device_info,
            group_list,
            do_info,
            di_list,
            user_table,
            account_table,
            notification_hist_table,
            req_datetime
        )

    # 履歴情報登録
    do_onoff_control = int(do_info["do_timer"]["do_onoff_control"])
    if do_onoff_control == 0:
        event_type = "on_timer_control"
        if flg == "__check_under_control":
            control_result = "not_excuted_done"
        else:
            control_result = "not_excuted_off"
    elif do_onoff_control == 1:
        event_type = "off_timer_control"
        if flg == "__check_under_control":
            control_result = "not_excuted_done"
        else:
            control_result = "not_excuted_on"
    elif do_onoff_control == 9:
        # ON/OFF指定されていない場合、不実施になるのは制御中のケースのみ
        event_type = "timer_control"
        control_result = "not_excuted_done"
    else:
        res_body = {"message": "接点出力_ON/OFF制御の値が不正です。"}
        respons["statusCode"] = 500
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons

    do_di_return = do_info["do_di_return"]
    if do_di_return != 0:
        link_terminal = [di for di in di_list if di["di_no"] == do_di_return][0]
        link_terminal_name = link_terminal["di_name"]
        if do_onoff_control == 0:
            link_terminal_state_name = link_terminal["di_on_name"]
            if not link_terminal_state_name:
                link_terminal_state_name = "クローズ"
        elif do_onoff_control == 1:
            link_terminal_state_name = link_terminal["di_off_name"]
            if not link_terminal_state_name:
                link_terminal_state_name = "オープン"
        elif do_onoff_control == 9:
            link_terminal_state_name = ""
    else:
        link_terminal_name = ""

    hist_data = {
        "device_name": device_info["device_data"]["config"]["device_name"],
        "group_list": group_list,
        "imei": device_info["imei"],
        "event_type": event_type,
        "terminal_no": int(do_info["do_no"]),
        "terminal_name": do_info["do_name"],
        "control_trigger": event_type,
        "link_terminal_no": int(do_di_return),
        "link_terminal_name": link_terminal_name,
        "link_terminal_state_name": link_terminal_state_name,
        "notification_hist_id": notification_hist_id,
        "control_result": control_result,
        "timer_time": do_info["do_timer"]["do_time"],
        "do_timer_name": do_info["do_timer"]["do_timer_name"],
    }

    if flg == "__check_under_control":
        hist_data["not_excuted_done_reason"] = reason

    if flg == "__check_return_di_state":
        if do_onoff_control == 0:
            hist_data["link_terminal_state_name"] = link_terminal["di_on_name"]
        elif do_onoff_control == 1:
            hist_data["link_terminal_state_name"] = link_terminal["di_off_name"]

    now_unixtime = int(time.time() * 1000)
    expire_datetime = int(
        (
            datetime.fromtimestamp(now_unixtime / 1000)
            + relativedelta.relativedelta(years=CNT_HIST_TTL)
        ).timestamp()
    )
    item = {
        "device_id": device_info["device_id"],
        "hist_id": str(uuid.uuid4()),
        "event_datetime": req_datetime,
        "expire_datetime": expire_datetime,
        "hist_data": hist_data,
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
        res_body = {"message": "履歴一覧情報への書き込みに失敗しました。"}
        respons["statusCode"] = 500
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        logger.info(respons)
        return respons
    logger.info(f"put_items: {put_items}")

    return True, result


def __send_mail(
    flg,
    reason,
    notification_setting,
    device_info,
    group_list,
    do_info,
    di_list,
    user_table,
    account_table,
    notification_hist_table,
    req_datetime
):
    # メール送信内容の設定
    req_datetime_converted = datetime.fromtimestamp(req_datetime / 1000.0,ZoneInfo("Asia/Tokyo"))
    send_datetime = datetime.now(ZoneInfo("Asia/Tokyo"))

    device_config = device_info.get("device_data", {}).get("config", {})
    device_name = (
        device_config.get("device_name")
        if device_config.get("device_name")
        else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{device_info.get("imei")}(IMEI)"
    )
    group_name_list = [g["group_name"] for g in group_list]
    group_name = "、".join(group_name_list)
    do_timer = do_info["do_timer"]["do_time"]
    do_timer_name = do_info["do_timer"]["do_timer_name"]
    if not do_timer_name:
        do_timer_name = "無題のスケジュール"

    # 接点出力名の設定
    do_name = do_info["do_name"]
    if not do_name:
        do_name = "接点出力" + str(do_info["do_no"])

    # 紐づく接点入力名・接点入力状態の設定
    do_di_return = do_info["do_di_return"]
    if do_di_return != 0:
        link_terminal = [di for di in di_list if di["di_no"] == do_di_return][0]
        di_name = link_terminal["di_name"]
        if not di_name:
            di_name = "接点入力" + str(link_terminal["di_no"])

    do_onoff_control = int(do_info["do_timer"]["do_onoff_control"])
    if do_onoff_control == 0:
        di_state = link_terminal["di_on_name"]
        if not di_state:
            di_state = "クローズ"
    elif do_onoff_control == 1:
        di_state = link_terminal["di_off_name"]
        if not di_state:
            di_state = "オープン"
    elif do_onoff_control == 9:
        di_state = ""

    mail_to_list = []
    for user_id in device_config.get("notification_target_list", []):
        mail_user = db.get_user_info_by_user_id(user_id, user_table)
        mail_account = db.get_account_info_by_account_id(mail_user["account_id"], account_table)
        mail_to_list.append(mail_account.get("email_address"))
    logger.debug(f"mail_to_list: {mail_to_list}")

    event_detail = ""
    if flg == "__check_return_di_state":
        event_detail = f"""
            　【スケジュール(不実施)】
            　{di_name}がすでに{di_state}のため、{do_name}のコントロールを行いませんでした。
            　 ※スケジュール「{di_state}コントロール ／ {do_timer}」
        """
    elif flg == "__check_under_control":
        if reason == "manual_control":
            event_detail = f"""
                　【スケジュール(不実施)】
                　他のユーザー操作により、{do_name}をコントロール中でした。
                　そのため、コントロールを行いませんでした。
                　 ※スケジュール「{do_timer_name} ／ {do_timer}」
            """
        elif reason in ["timer_control", "on_timer_control", "off_timer_control"]:
            event_detail = f"""
                　【スケジュール(不実施)】
                　スケジュールにより、{do_name}をコントロール中でした。
                　そのため、コントロールを行いませんでした。
                　 ※スケジュール「{do_timer_name} ／ {do_timer}」
            """
        else:
            event_detail = f"""
                　【スケジュール(不実施)】
                　オートメーションにより、{do_name}をコントロール中でした。
                　そのため、コントロールを行いませんでした。
                　 ※スケジュール「{do_timer_name} ／ {do_timer}」
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
