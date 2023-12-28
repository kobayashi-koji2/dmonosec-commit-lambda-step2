import logging
import os
import json
import re
import time
import traceback
import uuid

import boto3

# layer
import ssm
import validate
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
            account_table = dynamodb.Table(parameter["ACCOUNT_TABLE"])
            user_table = dynamodb.Table(parameter["USER_TABLE"])
            contract_table = dynamodb.Table(parameter["CONTRACT_TABLE"])
            device_relation_table = dynamodb.Table(parameter["DEVICE_RELATION_TABLE"])
            device_table = dynamodb.Table(parameter["DEVICE_TABLE"])
            req_no_counter_table = dynamodb.Table(parameter["REQ_NO_COUNTER_TABLE"])
            remote_controls_table = dynamodb.Table(parameter["REMOTE_CONTROL_TABLE"])
            hist_list_table = dynamodb.Table(parameter.get("HIST_LIST_TABLE"))
            group_table = dynamodb.Table(parameter.get("GROUP_TABLE"))
        except KeyError as e:
            parameter = None
            res_body = {"code": "9999", "message": e}
            respons["statusCode"] = 500
            return respons

        ### 1. 入力情報チェック
        # 入力情報のバリデーションチェック
        val_result = validate.validate(event, account_table, user_table)
        if val_result["code"] != "0000":
            print("Error in validation check of input information.")
            respons["statusCode"] = 500
            respons["body"] = json.dumps(val_result, ensure_ascii=False)
            return respons
        user_name = val_result["account_info"]["user_data"]["config"]["user_name"]
        email_address = val_result["account_info"]["email_address"]

        ### 2. デバイス捜査権限チェック（共通）
        # デバイスID一覧取得
        contract_id = val_result["user_info"]["contract_id"]
        contract_info = db.get_contract_info(contract_id, contract_table)
        if not "Item" in contract_info:
            res_body = {"code": "9999", "message": "契約情報が存在しません。"}
            respons["statusCode"] = 500
            respons["body"] = json.dumps(res_body, ensure_ascii=False)
            return respons
        contract_info = contract_info["Item"]
        print("contract_info", end=": ")
        print(contract_info)
        device_list = contract_info["contract_data"]["device_list"]
        
        # デバイス操作権限チェック
        device_id = val_result["path_params"]["device_id"]
        if device_id not in device_list:
            res_body = {"code": "9999", "message": "デバイスの操作権限がありません。"}
            respons["statusCode"] = 500
            respons["body"] = json.dumps(res_body, ensure_ascii=False)
            return respons
        
        ### 3. デバイス捜査権限チェック（作業者の場合）
        # デバイス操作権限チェック
        user_info = val_result["user_info"]
        if user_info["user_type"] == "worker":
            pk = "u-" + user_info["user_id"]
            relation_info = db.get_device_relation(pk, device_relation_table)
            print("device_relation", end=": ")
            print(relation_info)
            relation_d = [i["key2"] for i in relation_info if i["key2"].startswith("d-")]
            relation_g = [i["key2"] for i in relation_info if i["key2"].startswith("g-")]

            # グル―プにも紐づいている場合、そのグループに紐づくデバイスを取得
            if len(relation_g) != 0:
                for pk in relation_g:
                    relation_info = db.get_device_relation(pk, device_relation_table)
                    print("device_relation[g-]", end=": ")
                    print(relation_info)
                    relation_d += [i["key2"] for i in relation_info]
            device_id_list = [i.replace("d-","",1) for i in list(dict.fromkeys(relation_d))]
            print("device_id_list", end=": ")
            print(device_id_list)

            if device_id not in device_id_list:
                res_body = {"code": "9999", "message": "デバイスの操作権限がありません。"}
                respons["statusCode"] = 500
                respons["body"] = json.dumps(res_body, ensure_ascii=False)
                return respons
        else:
            pass

        ### 4. 制御情報取得
        device_info = ddb.get_device_info_other_than_unavailable(device_id, device_table)
        if len(device_info) == 0:
            res_body = {"code": "9999", "message": "デバイス情報が存在しません。"}
            respons["statusCode"] = 500
            respons["body"] = json.dumps(res_body, ensure_ascii=False)
            return respons
        device_info = device_info[0]
        print("device_info", end=": ")
        print(device_info)

        ### 5. 制御中判定
        # 要求番号取得
        icc_id = device_info["device_data"]["param"]["iccid"]
        do_no = int(val_result["path_params"]["do_no"])
        req_no_count_info = ddb.get_req_no_count_info(icc_id, req_no_counter_table)
        if req_no_count_info:
            print("req_no_count_info", end=": ")
            print(req_no_count_info)

            # 最新制御情報取得
            latest_req_num = convert.decimal_default_proc(req_no_count_info["num"])
            latest_req_no = format(latest_req_num % 65535, "#04x")
            device_req_no = icc_id + "-" + latest_req_no
            remote_control_latest = ddb.get_remote_control_latest(device_req_no, do_no, remote_controls_table)
            if len(remote_control_latest) == 0:
                res_body = {"code": "9999", "message": "接点出力制御応答情報が存在しません。"}
                respons["statusCode"] = 500
                respons["body"] = json.dumps(res_body, ensure_ascii=False)
                return respons
            remote_control_latest = remote_control_latest[0]
            print("remote_control_latest", end=": ")
            print(remote_control_latest)

            # 制御中判定
            if ("recv_datetime" not in remote_control_latest) or (remote_control_latest["recv_datetime"] == 0):
                print("Not processed because recv_datetime exists in remote_control_latest (judged as under control)")
                __register_hist_info(
                        device_info, do_no, user_name, email_address,
                        group_table, device_relation_table, hist_list_table
                )
                res_body = {"code": "9999", "message": "他のユーザー操作、タイマーまたは連動により制御中"}
                respons["body"] = json.dumps(res_body, ensure_ascii=False)
                return respons

            ### 6. 要求番号生成（アトミックカウンタをインクリメントし、端末要求番号を生成）
            req_num = ddb.increment_req_no_count_num(icc_id, req_no_counter_table)
            req_no = format(req_num % 65535, "#04x")

        else:
            ### 6. 要求番号生成（カウント0 のレコード作成し、カウント0 の端末要求番号を生成）
            print("req_no_count_info did not exist. Put req_no_count_info to table")
            req_num = 0 
            write_items = [{
                "Put": {
                    "TableName": parameter["REQ_NO_COUNTER_TABLE"],
                    "Item": {
                        "simid": {"S": icc_id},
                        "num": {"N": str(req_num)}
                    },
                }
            }]
            result = db.execute_transact_write_item(write_items)
            if not result:
                res_body = {"code": "9999", "message": "要求番号カウンタ情報への書き込みに失敗しました。"}
                respons["statusCode"] = 500
                respons["body"] = json.dumps(res_body, ensure_ascii=False)
                return respons
            req_no = format(req_num % 65535, "#04x")

        ### 7. 接点出力制御要求
        # 接点出力制御要求メッセージを生成
        topic = "cmd/" + icc_id
        # 接点出力_制御状態・接点出力_制御時間を判定
        do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
        do_info = [do for do in do_list if int(do["do_no"]) == do_no][0]
        do_specified_time = convert.decimal_default_proc(do_info["do_specified_time"])
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
        # iot_result = iot.publish(
        #     topic=topic,
        #     qos=0,
        #     payload=json.dumps(payload, ensure_ascii=False)
        # )
        # print("iot_result", end=": ")
        # print(iot_result)

        # 要求データを接点出力制御応答TBLへ登録
        device_req_no = icc_id + "-" + req_no
        do_di_return = convert.decimal_default_proc(do_info["do_di_return"])
        put_items = [{
            "Put": {
                "TableName": parameter["REMOTE_CONTROL_TABLE"],
                "Item": {
                    "device_req_no": {"S": device_req_no},
                    "req_datetime": {"N": str(int(time.time() * 1000))},
                    "device_id": {"S": device_id},
                    "contract_id": {"S": contract_id},
                    "control": {"S": do_info["do_control"]},
                    "control_trigger": {"S": "manual_control"},
                    "do_no": {"N": str(do_no)},
                    "link_di_no": {"S": str(do_di_return)},
                    "iccid": {"S": icc_id},
                    "control_exec_user_name": {"S": user_name},
                    "control_exec_email_address": {"S": email_address},
                },
            }
        }]
        result = db.execute_transact_write_item(put_items)
        if not result:
            res_body = {"code": "9999", "message": "接点出力制御応答情報への書き込みに失敗しました。"}
            respons["statusCode"] = 500
            respons["body"] = json.dumps(res_body, ensure_ascii=False)
            return respons

        ### 8. タイムアウト判定Lambda呼び出し
        # payload = {
        #     "headers": event["headers"],
        #     "body": {"device_req_no": device_req_no}
        # }
        # lambda_invoke_result = aws_lambda.invoke(
        #     FunctionName = LAMBDA_TIMEOUT_CHECK,
        #     InvocationType="Event",
        #     Payload = json.dumps(payload, ensure_ascii=False)
        # )
        # print("lambda_invoke_result", end=": ")
        # print(lambda_invoke_result)

        ### 9. メッセージ応答
        res_body = {
            "code": "0000",
            "message": "",
            "device_req_no": device_req_no
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


def __register_hist_info(
    device_info, do_no, user_name, email_address,
    group_table, device_relation_table, hist_list_table
):
    """
    - 要求番号が設定されており、接点出力端子が制御中の場合
        履歴情報一覧へ実施しなかったことを登録する
    """
    result = None
    do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
    do_info = [do for do in do_list if int(do["do_no"]) == do_no][0]

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
                respons["statusCode"] = 500
                respons["body"] = json.dumps(result, ensure_ascii=False)
                return respons
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
            "control_exec_email_address": email_address,
            "notification_hist_id": notification_hist_id,
            "control_result": "not_excuted_done",
        }
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
