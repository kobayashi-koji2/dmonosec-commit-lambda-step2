import logging
import os
import json
import traceback
from decimal import Decimal

import boto3

# layer
import ssm
import validate
import db

logger = logging.getLogger()

# 環境変数
parameter = None
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
ENDPOINT_URL = os.environ["endpoint_url"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
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
dynamodb = boto3.resource("dynamodb",region_name=AWS_DEFAULT_REGION,endpoint_url=ENDPOINT_URL)

def lambda_handler(event, context):
    try:
        ### 0. 環境変数の取得・DynamoDBの操作オブジェクト生成
        global parameter
        if parameter is None:
            ssm_params = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(ssm_params)
        else:
            print("parameter already exists. pass get_ssm_parameter")

        account_table = dynamodb.Table(parameter.get("ACCOUNT_TABLE"))
        user_table = dynamodb.Table(parameter["USER_TABLE"])
        device_relation_table = dynamodb.Table(parameter["DEVICE_RELATION_TABLE"])
        contract_table = dynamodb.Table(parameter["CONTRACT_TABLE"])
        device_table = dynamodb.Table(parameter["DEVICE_TABLE"])
        device_state_table = dynamodb.Table(parameter["STATE_TABLE"])

        ### 1. 入力情報チェック
        # 入力情報のバリデーションチェック
        val_result = validate.validate(event, user_table)
        if val_result["code"] != "0000":
            print("Error in validation check of input information.")
            respons["statusCode"] = 500
            respons["body"] = json.dumps(val_result, ensure_ascii=False)
            return respons
        # トークンからユーザー情報取得
        user_info = val_result["user_info"]["Item"]
        print("user_info", end=": ")
        print(user_info)
        # ユーザー権限確認
        # 1月まではいったん、ログインするユーザーIDとモノセコムユーザーIDは同じ認識で直接ユーザー管理より参照する形で実装
        # バリデーションチェックの処理の中でモノセコムユーザー管理より参照しているのでその値を使用

        ### 2. デバイスID取得（作業者・参照者の場合）
        device_id_list = list()
        if user_info["user_type"] in ("worker", "referrer"):
            print("In case of worker/referee")
            device_id_list = __get_device_id_in_case_of_worker_or_referee(user_info, device_relation_table)

        ### 3. デバイスID取得（管理者・副管理者の場合）
        if user_info["user_type"] in ("admin", "sub_admin"):
            print("In case of admin/sub_admin")
            cotract_id = user_info["contract_id"]
            contract_info = db.get_contract_info(cotract_id, contract_table).get("Item")
            print("contract_info", end=": ")
            print(contract_info)
            device_id_list = contract_info["contract_data"]["device_list"]

        print("device_id_list", end=": ")
        print(device_id_list)

        ### 4. 遠隔制御一覧生成
        results = list()
        # デバイス情報取得
        for device_id in device_id_list:
            device_info = db.get_device_info(device_id, device_table)
            print("device_info", end=": ")
            print(device_info)
            
            if device_info is not None:
                # 現状態情報取得
                state_info = db.get_device_state(device_id, device_state_table).get("Item")
                print("state_info", end=": ")
                print(state_info)
                device_imei = device_info["imei"]
                device_name = device_info["device_data"]["config"]["device_name"]
                # 接点出力一覧
                do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
                # 接点入力一覧
                di_list = device_info["device_data"]["config"]["terminal_settings"]["di_list"]

                # 接点出力を基準にそれに紐づく接点入力をレスポンス内容として設定
                for do_info in do_list:
                    res_item = __generate_response_items(
                        device_id,
                        device_name,
                        device_imei,
                        do_info,
                        di_list,
                        state_info
                    )
                    results.append(res_item)

        ### 5. メッセージ応答
        results = __decimal_to_integer_or_float(results)
        print("results", end=": ")
        print(results)
        res_body = {
            "code": "0000",
            "message": "",
            "remoto_control_list": results
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


def __get_device_id_in_case_of_worker_or_referee(user_info, device_relation_table):
    device_id_list = []
    # ユーザーIDに紐づくグループIDからデバイスIDを取得（デバイス関係TBL）
    quary_item = "u-" + user_info["user_id"]
    kwargs = {"sk_prefix": "g-"}
    device_relation_results = db.get_device_relation(
        quary_item,
        device_relation_table,
        **kwargs
    )
    print("device_relation[g-]", end=": ")
    print(device_relation_results)
    for group_id in device_relation_results:
        kwargs = {"sk_prefix": "d-"}
        device_relation_results = db.get_device_relation(
            group_id["key2"],
            device_relation_table,
            **kwargs
        )
        device_id_list += [item["key2"] for item in device_relation_results]
        print("device_relation[g-d-]", end=": ")
        print(device_relation_results)

    # ユーザーIDに紐づくデバイスIDを取得（デバイス関係TBL）
    kwargs = {"sk_prefix": "d-"}
    device_relation_results = db.get_device_relation(
        quary_item,
        device_relation_table,
        **kwargs
    )
    print("device_relation[d-]", end=": ")
    print(device_relation_results)
    device_id_list += [item["key2"] for item in device_relation_results]
    device_id_list = [i[2:] for i in list(dict.fromkeys(device_id_list))]
    print("device_id_list", end=": ")
    print(device_id_list)

    return device_id_list


def __generate_response_items(device_id, device_name, device_imei, do_info, di_list, state_info):
    results_item = dict()
    results_item["device_id"] = device_id
    results_item["device_name"] = device_name
    results_item["device_imei"] = device_imei
    # 接点出力情報を設定
    results_item["do_no"] = do_info["do_no"]
    results_item["do_name"] = do_info["do_name"]
    if do_info["do_control"] == "toggle":
        results_item["do_control"] = 0
    elif do_info["do_control"] == "close":
        results_item["do_control"] = 1
    elif do_info["do_control"] == "open":
        results_item["do_control"] = 2

    # 接点出力情報をもとに接点入力情報を設定
    if not do_info["do_di_return"]:
        results_item["di_no"] = ""
        results_item["di_name"] = ""
        results_item["di_state_name"] = ""
        results_item["di_state_icon"] = ""
    else:
        di_info = list(
            filter(lambda i : i["di_no"] == do_info["do_di_return"], di_list)
        )[0]
        di_number = di_info["di_no"]
        results_item["di_no"] = di_number
        
        # 接点入力名が未設定の場合「接点入力{接点入力端子番号}」で設定
        if not di_info["di_name"]:
            results_item["di_name"] = "接点入力" + str(di_number)
        else:
            results_item["di_name"] = di_info["di_name"]
        
        # 「接点入力状態名称・接点入力状態アイコン」は現状態TBLの「接点入力{接点入力端子番号}_現状態」に対応する値を設定
        if state_info[f"di{di_number}_state"] == 0:
            results_item["di_state_name"] = di_info["di_off_name"]
            results_item["di_state_icon"] = di_info["di_off_icon"]
        else:
            results_item["di_state_name"] = di_info["di_on_name"]
            results_item["di_state_icon"] = di_info["di_on_icon"]

    return results_item


# dict型のDecimalを数値に変換
def __decimal_to_integer_or_float(param):
    if isinstance(param, dict):
        for key, value in param.items():
            if isinstance(value, Decimal):
                if value % 1 == 0:
                    param[key] = int(value)
                else:
                    param[key] = float(value)
            else:
                __decimal_to_integer_or_float(value)
    elif isinstance(param, list):
        for item in param:
            __decimal_to_integer_or_float(item)
    return param
