from datetime import datetime, timedelta
import logging
import os
import json
import time
import traceback
import uuid
import re

import boto3

# layer
import ssm
import db
import convert
import ddb

logger = logging.getLogger()

# 環境変数
parameter = None
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
LAMBDA_TIMEOUT_CHECK = os.environ["LAMBDA_TIMEOUT_CHECK"]
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
    "dynamodb",
    region_name=AWS_DEFAULT_REGION,
    endpoint_url=os.environ.get("endpoint_url")
)
iot = boto3.client("iot-data",region_name=AWS_DEFAULT_REGION)
aws_lambda = boto3.client("lambda",region_name=AWS_DEFAULT_REGION)

def lambda_handler(event, context):
    try:
        ### 0. 環境変数の取得・DynamoDBの操作オブジェクト生成
        global parameter
        if parameter is None:
            ssm_params = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(ssm_params)
        else:
            print("parameter already exists. pass get_ssm_parameter")

        try:
            device_table = dynamodb.Table(parameter["DEVICE_TABLE"])
            device_state_table = dynamodb.Table(parameter["STATE_TABLE"])
            req_no_counter_table = dynamodb.Table(parameter["REQ_NO_COUNTER_TABLE"])
            remote_controls_table = dynamodb.Table(parameter["REMOTE_CONTROL_TABLE"])
            hist_list_table = dynamodb.Table(parameter.get("HIST_LIST_TABLE"))
            device_relation_table = dynamodb.Table(parameter["DEVICE_RELATION_TABLE"])
            group_table = dynamodb.Table(parameter.get("GROUP_TABLE"))
        except KeyError as e:
            parameter = None
            res_body = {"code": "9999", "message": e}
            respons["statusCode"] = 500
            return respons

        ### 1. スケジュール設定チェック
        # 現在時刻の保持(1分ごとに実行)
        dt_now = datetime.now()
        if dt_now.tzname != "JST":
            dt_now = dt_now + timedelta(hours=+9)
        print("now_time", end=": ")
        print(dt_now.strftime("%H:%M"))

        # 実行対象のデバイス情報取得
        device_info_list = ddb.get_device_info_available(device_table)
        # 有効デバイス有無チェック
        if len(device_info_list) == 0:
            # 正常終了
            res_body = {"code": "0000", "message": ""}
            respons["body"] = json.dumps(res_body, ensure_ascii=False)
            return respons

        ### 2. 接点出力制御要求
        for device_info in device_info_list:
            print("--- device_info", end=": ")
            print(device_info)
            device_id = device_info["device_id"]
            contract_id = device_info["device_data"]["param"]["contract_id"]
            icc_id = device_info["device_data"]["param"]["iccid"]
            do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
            di_list = device_info["device_data"]["config"]["terminal_settings"]["di_list"]

            for do_info in do_list:
                # タイマー設定チェック
                checked_timer_do_info = __check_timer_settings(do_info, dt_now)
                if not checked_timer_do_info:
                    print(f"[__check_timer_settings(): FALSE] device_id: {device_id}, do_info", end=": ")
                    print(do_info)
                    continue

                # 接点入力状態チェック
                error_flg, result = __check_return_di_state(
                    checked_timer_do_info, device_id, device_state_table
                )
                if not error_flg:
                    respons["statusCode"] = 500
                    respons["body"] = json.dumps(result, ensure_ascii=False)
                    return respons
                if result == 1:
                    error_flg, result = __register_hist_info(
                        "__check_return_di_state",
                        device_info, do_info, di_list,
                        group_table, device_relation_table, hist_list_table
                    )
                    print(f"[__check_return_di_state(): FALSE] device_id: {device_id}, do_info", end=": ")
                    print(do_info)
                    continue
                elif not result:
                    print(f"[__check_return_di_state(): FALSE] device_id: {device_id}, do_info", end=": ")
                    print(do_info)
                    continue
                checked_di_state_info = result

                # 制御中判定
                error_flg, result = __check_under_control(
                    checked_di_state_info, icc_id, req_no_counter_table, remote_controls_table
                )
                if not error_flg:
                    respons["statusCode"] = 500
                    respons["body"] = json.dumps(result, ensure_ascii=False)
                    return respons
                if result == 1:
                    error_flg, result = __register_hist_info(
                        "__check_under_control", device_info, do_info, di_list,
                        group_table, device_relation_table, hist_list_table
                    )
                    if not error_flg:
                        respons["statusCode"] = 500
                        respons["body"] = json.dumps(result, ensure_ascii=False)
                        return respons
                    print(f"[__check_under_control(): FALSE] device_id: {device_id}, do_info", end=": ")
                    print(do_info)
                    continue
                elif not result:
                    print(f"[__check_under_control(): FALSE] device_id: {device_id}, do_info", end=": ")
                    print(do_info)
                    continue
                checked_under_control_info = result

                # 端末向け要求番号生成
                req_no = format(checked_under_control_info["req_num"] % 65535, "#04x")

                # 接点出力制御要求メッセージを生成
                topic = "cmd/" + icc_id
                do_no = int(do_info["do_no"])
                do_specified_time = int(do_info["do_specified_time"])

                if do_info["do_control"] == "open":
                    do_control = "0x00"
                    do_control_time = format(do_specified_time, "#04x")
                elif do_info["do_control"] == "close":
                    do_control = "0x01"
                    do_control_time = format(do_specified_time, "#04x")
                elif do_info["do_control"] == "toggle":
                    do_control = "0x10"
                    do_control_time = "0x0000"
                else:
                    res_body = {"code": "9999", "message": "接点出力_制御方法の値が不正です。"}
                    respons["statusCode"] = 500
                    respons["body"] = json.dumps(res_body, ensure_ascii=False)
                    return respons

                payload = {
                    "Message_Length": "0x000C",
                    "Message_type": "0x8002",
                    "Req_No": req_no,
                    "DO_No": format(do_no, "#04x"),
                    "DO_Control": do_control,
                    "DO_ControlTime": do_control_time
                }
                print("Iot Core Message", end=": ")
                print(payload)

                # AWS Iot Core へメッセージ送信
                iot_result = iot.publish(
                    topic=topic,
                    qos=0,
                    payload=json.dumps(payload, ensure_ascii=False)
                )
                print("iot_result", end=": ")
                print(iot_result)

                # 要求データを接点出力制御応答TBLへ登録
                device_req_no = icc_id + "-" + req_no
                do_di_return = int(do_info["do_di_return"])
                do_onoff_control = int(do_info["do_timer"]["do_onoff_control"])
                if do_onoff_control == 0:
                    control_trigger = "off_timer_control"
                elif do_onoff_control == 1:
                    control_trigger = "on_timer_control"
                else:
                    res_body = {"code": "9999", "message": "接点出力_ON/OFF制御の値が不正です。"}
                    respons["statusCode"] = 500
                    respons["body"] = json.dumps(res_body, ensure_ascii=False)
                    return respons
                
                put_items = [{
                    "Put": {
                        "TableName": remote_controls_table.name,
                        "Item": {
                            "device_req_no": {"S": device_req_no},
                            "req_datetime": {"N": str(int(time.time() * 1000))},
                            "device_id": {"S": device_id},
                            "contract_id": {"S": contract_id},
                            "control": {"S": do_info["do_control"]},
                            "control_trigger": {"S": control_trigger},
                            "do_no": {"N": str(do_no)},
                            "link_di_no": {"N": do_di_return},
                            "iccid": {"S": icc_id}
                        },
                    }
                }]
                result = db.execute_transact_write_item(put_items)
                if not result:
                    res_body = {"code": "9999", "message": "接点出力制御応答情報への書き込みに失敗しました。"}
                    respons["statusCode"] = 500
                    respons["body"] = json.dumps(res_body, ensure_ascii=False)
                    return respons
                print("put_items", end=": ")
                print(put_items)

                # タイムアウト判定Lambda呼び出し
                payload = {
                    "body": {"device_req_no": device_req_no}
                }
                lambda_invoke_result = aws_lambda.invoke(
                    FunctionName = LAMBDA_TIMEOUT_CHECK,
                    InvocationType="Event",
                    Payload = json.dumps(payload, ensure_ascii=False)
                )
                print("lambda_invoke_result", end=": ")
                print(lambda_invoke_result)

        ### 3. メッセージ応答
        res_body = {
            "code": "0000",
            "message": ""
        }
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        res_body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        respons["statusCode"] = 500
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons


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
            if (do_time.hour == dt_now.hour) and (do_time.minute == dt_now.minute):
                do_info["do_timer"] = do_timer
                result = do_info
                # 同一接点出力端子で重複した設定は不可となるので１つだけ抽出とする
                break
    return result


def __check_return_di_state(do_info, device_id, device_state_table):
    """
    1. 紐づく接点入力端子番号の指定がある場合
        その出力端子に紐づく接点入力端子の現状態を確認し、タイマーのON_OFF制御と比較検証する。
            1. タイマーのON_OFF制御と紐づく接点入力端子の現状態の値が一致しない場合
                処理続行する。
            2. タイマーのON_OFF制御と紐づく接点入力端子の現状態の値が一致する場合
                履歴情報を登録して処理対象外としてスキップする。                
    2. 紐づく接点入力端子番号の指定がない場合
        処理対象外としてスキップする。
    """
    result = None

    if ("do_di_return" in do_info) and do_info["do_di_return"]:
        device_state_info = db.get_device_state(device_id, device_state_table)
        if not "Item" in device_state_info:
            res_body = {"code": "9999", "message": "現状態情報が存在しません。"}
            return False, res_body
        device_state_info = device_state_info["Item"]
        print("device_state_info", end=": ")
        print(device_state_info)

        # タイマーのON/OFF制御と接点入力状態の値が一致するなら処理対象外
        col_name = "di" + str(do_info["do_di_return"]) + "_state"
        if do_info["do_timer"]["do_onoff_control"] != device_state_info[col_name]:
            result = do_info
        else:
            print(f"Not processed because the values of do_onoff_control and {col_name} match")
            result = 1
    else:
        print("Not processed because do_di_return is not set")
        pass

    return True, result


def __check_under_control(
        do_info, icc_id, req_no_counter_table, remote_controls_table
    ):
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

    req_no_count_info = ddb.get_req_no_count_info(icc_id, req_no_counter_table)
    if req_no_count_info:
        print("req_no_count_info", end=": ")
        print(req_no_count_info)

        # 最新制御情報取得
        latest_req_no = format(int(req_no_count_info["num"]) % 65535, "#04x")
        device_req_no = icc_id + "-" + latest_req_no
        remote_control_latest = ddb.get_remote_control_latest(
            device_req_no, do_info["do_no"], remote_controls_table
        )
        if len(remote_control_latest) == 0:
            res_body = {"code": "9999", "message": "接点出力制御応答情報が存在しません。"}
            return False, res_body
        remote_control_latest = remote_control_latest[0]
        print("remote_control_latest", end=": ")
        print(remote_control_latest)

        # 制御中判定
        if ("recv_datetime" not in remote_control_latest) or (remote_control_latest["recv_datetime"] == 0):
            print("Not processed because recv_datetime exists in remote_control_latest (judged as under control)")
            return True, 1

        # 要求番号生成（アトミックカウンタをインクリメントし、要求番号を取得）
        req_num = ddb.increment_req_no_count_num(icc_id, req_no_counter_table)
        result = do_info
        result["req_num"] = int(req_num)
    
    else:
        print("req_no_count_info did not exist. Put req_no_count_info to table")
        req_num = 0 
        write_items = [{
            "Put": {
                "TableName": req_no_counter_table.name,
                "Item": {
                    "simid": {"S": icc_id},
                    "num": {"N": str(req_num)}
                },
            }
        }]
        result = db.execute_transact_write_item(write_items)
        if not result:
            res_body = {"code": "9999", "message": "要求番号カウンタ情報への書き込みに失敗しました。"}
            return False, res_body
        result = do_info
        result["req_num"] = req_num

    return True, result


def __register_hist_info(
    flg,
    device_info, do_info, di_list,
    group_table, device_relation_table, hist_list_table
):
    """
    1. 紐づく接点入力端子番号の指定があり、その出力端子の現状態ステータスがタイマーのON_OFF制御の値と一致する場合
    2. 要求番号が設定されており、接点出力端子が制御中の場合

    - 履歴情報一覧へ実施しなかったことを登録する
    """
    result = None

    # グループ情報取得
    group_list = list()
    pk = "d-" + device_info["device_id"]
    group_device_list = db.get_device_relation(pk, device_relation_table, sk_prefix="g-", gsi_name="key2_index")
    if len(group_device_list) != 0:
        group_id_list = [
            re.sub("^g-", "", group_device_info["key1"])
            for group_device_info in group_device_list
        ]
        for group_id in group_id_list:
            group_info = db.get_group_info(group_id, group_table)
            if not "Item" in group_info:
                res_body = {"code": "9999", "message": "グループ情報が存在しません。"}
                return False, res_body
            print("group_info", end=": ")
            print(group_info)
            group_list.append(
                {
                    "group_id": group_info["Item"]["group_id"],
                    "group_name": group_info["Item"]["group_data"]["config"]["group_name"]
                }   
            )
    else:
        print("The group containing the device did not exist.")
        group_list.append({"group_id": "", "group_name": ""})

    # メール通知
    notification_hist_id = ""
    # 12月の段階ではスキップ

    do_di_return = do_info["do_di_return"]
    link_terminal_name = [di["di_name"] for di in di_list if di["di_no"] == do_di_return][0]
    do_onoff_control = int(do_info["do_timer"]["do_onoff_control"])
    if do_onoff_control == 0:
        event_type = "off_timer_control"
        control_result = "not_excuted_on"
    elif do_onoff_control == 1:
        event_type = "on_timer_control"
        control_result = "not_excuted_off"
    else:
        res_body = {"code": "9999", "message": "接点出力_ON/OFF制御の値が不正です。"}
        respons["statusCode"] = 500
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons

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
        "notification_hist_id": notification_hist_id,
        "control_result": control_result,
        "timer_time": do_info["do_timer"]["do_time"],
    }
    item = {
        "device_id": device_info["device_id"],
        "hist_id": str(uuid.uuid4()),
        "event_datetime": int(time.time() * 1000),
        "hist_data": hist_data
    }
    item = convert.dict_dynamo_format(item)
    put_items = [{
        "Put": {
            "TableName": hist_list_table.name,
            "Item": item,
        }
    }]
    result = db.execute_transact_write_item(put_items)
    if not result:
        res_body = {"code": "9999", "message": "履歴一覧情報への書き込みに失敗しました。"}
        respons["statusCode"] = 500
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons
    print("put_items", end=": ")
    print(put_items)

    return True, result