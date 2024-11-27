import os
import json
import traceback
import re
from decimal import Decimal

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from functools import reduce
import boto3
import copy

# layer
import auth
import ssm
import db
import ddb

patch_all()

logger = Logger()

# 環境変数
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]

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


@auth.verify_login_user()
def lambda_handler(event, context, user_info):
    try:
        ### 0. DynamoDBの操作オブジェクト生成
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        device_state_table = dynamodb.Table(ssm.table_names["STATE_TABLE"])
        group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])

        logger.debug(f"user_info: {user_info}")

        cotract_id = user_info["contract_id"]

        ### 2. デバイスID取得（作業者・参照者の場合）
        device_id_list = list()
        if user_info["user_type"] in ("worker", "referrer"):
            logger.info("In case of worker/referee")
            device_id_list = db.get_user_relation_device_id_list(
                user_info["user_id"], device_relation_table
            )

        ### 3. デバイスID取得（管理者・副管理者の場合）
        if user_info["user_type"] in ("admin", "sub_admin"):
            logger.info("In case of admin/sub_admin")
            contract_info = db.get_contract_info(cotract_id, contract_table)
            logger.debug(f"contract_info: {contract_info}")
            device_id_list = contract_info["contract_data"]["device_list"]

        logger.debug(f"device_id_list: {device_id_list}")

        ### 4. グループ名一覧取得
        # グループID取得
        device_group_relation, all_groups = (
            [],
            [],
        )  # デバイスID毎のグループID一覧、重複のないグループID一覧
        for device_id in device_id_list:
            group_id_list = db.get_device_relation_group_id_list(
                device_id, device_relation_table
            )
            device_group_relation.append({"device_id": device_id, "group_list": group_id_list})
            all_groups += group_id_list
        all_groups = set(all_groups)
        logger.info(f"デバイスグループ関連:{device_group_relation}")
        logger.info(f"重複のないグループID一覧:{all_groups}")

        # グループ情報取得
        group_info_list = []
        for item in all_groups:
            group_info = db.get_group_info(item, group_table)
            if group_info:
                group_info_list.append(group_info)
            else:
                logger.info(f"group information does not exist:{item}")
        logger.info(f"グループ情報:{group_info_list}")
        if group_info_list:
            group_info_list = sorted(
                group_info_list, key=lambda x: x["group_data"]["config"]["group_name"]
            )

        device_info_list, device_info_all_list = [], []
        device_info_all_list = ddb.get_device_info_only_pj2_by_contract_id(cotract_id, device_table)
        device_info_all_list = db.insert_id_key_in_device_info_list(device_info_all_list)

        for device_item in device_id_list:
            for device_info_item in device_info_all_list:
                if device_info_item["device_id"] == device_item:
                    do_list = device_info_item["device_data"]["config"]["terminal_settings"]["do_list"]
                    for do_info in do_list:
                        if not do_info["do_control"]:
                            continue
                        device_info_item["device_data"]["config"]["terminal_settings"]["do_info"] = do_info
                        logger.info(f"device_info_item:{device_info_item}")
                        copy_item = copy.deepcopy(device_info_item)
                        device_info_list.append(copy_item)
                    break

        logger.info(f"device_info_list:{device_info_list}")

        detect_condition = None
        keyword = None
        query_params = event.get("queryStringParameters")
        if query_params:
            keyword = query_params.get("keyword")
            query_param_detect_condition = query_params.get("detect_condition")
            if query_param_detect_condition:
                if query_param_detect_condition.isdecimal():
                    detect_condition = int(query_param_detect_condition)

        if keyword == None or keyword == "":
            device_info_list_filtered = device_info_list
        elif detect_condition != None:
            device_info_list_filtered = keyword_detection_device_list(detect_condition, keyword, device_info_list, group_info_list, device_group_relation)
        else:
            res_body = {"message": "検索条件が設定されていません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        ### 5. 遠隔制御一覧生成
        results = list()
        # デバイス情報取得
        for device_info in device_info_list_filtered:
            logger.debug({"device_info": device_info})
            if device_info:
                pass
            elif not device_info:
                logger.info(f"device information does not exist:{device_info["device_id"]}")
                continue

            if device_info is not None and device_info.get("device_type") in ["PJ1", "PJ2", "PJ3"]:
                # 現状態情報取得
                state_info = db.get_device_state(device_info["device_id"], device_state_table)
                # 現状態情報がない場合は次のデバイスへ
                if not state_info:
                    continue
                logger.debug(f"state_info: {state_info}")
                device_imei = device_info["imei"]
                device_code = device_info.get("device_data", {}).get("param", {}).get("device_code", ""),
                device_name = (
                    device_info.get("device_data", {}).get("config", {}).get("device_name", "")
                )
                # 接点入力一覧
                di_list = device_info["device_data"]["config"]["terminal_settings"]["di_list"]

                # グループ情報取得
                filtered_device_group_relation = next(
                    (group for group in device_group_relation if group["device_id"] == device_info["device_id"]), {}
                ).get("group_list", [])
                logger.info(f"グループID参照:{filtered_device_group_relation}")
                # グループ名参照
                group_name_list = []
                for group_id in filtered_device_group_relation:
                    group_name_list.append(
                        next((group for group in group_info_list if group["group_id"] == group_id), {})
                        .get("group_data", {})
                        .get("config", {})
                        .get("group_name", "")
                    )
                if group_name_list:
                    group_name_list.sort()
                logger.info(f"グループ名:{group_name_list}")

                # 接点出力を基準にそれに紐づく接点入力をレスポンス内容として設定
                do_info = device_info["device_data"]["config"]["terminal_settings"]["do_info"]
                res_item = __generate_response_items(
                    device_info["device_id"], device_name, device_imei, device_code, do_info, di_list, state_info, group_name_list
                )
                results.append(res_item)
                

        ### 6. メッセージ応答
        results = __decimal_to_integer_or_float(results)
        logger.info({"results": results})
        res_body = {"message": "", "remote_control_list": results}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }


def __generate_response_items(device_id, device_name, device_imei, device_code, do_info, di_list, state_info, group_name_list):
    results_item = dict()
    results_item["device_id"] = device_id
    results_item["device_name"] = device_name
    results_item["device_imei"] = device_imei
    results_item["device_code"] = device_code
    # 接点出力情報を設定
    results_item["do_no"] = do_info["do_no"]
    results_item["do_name"] = do_info["do_name"]
    if do_info["do_control"] == "toggle":
        results_item["do_control"] = 0
    elif do_info["do_control"] == "open":
        results_item["do_control"] = 1
    elif do_info["do_control"] == "close":
        results_item["do_control"] = 2

    # 接点出力情報をもとに接点入力情報を設定
    if not do_info["do_di_return"]:
        results_item["di_no"] = ""
        results_item["di_name"] = ""
        results_item["di_state_name"] = ""
        results_item["di_state_icon"] = ""
    else:
        di_info = list(filter(lambda i: i["di_no"] == do_info["do_di_return"], di_list))[0]
        di_number = di_info["di_no"]
        results_item["di_no"] = di_number

        # 接点入力名が未設定の場合「接点入力{接点入力端子番号}」で設定
        if not di_info["di_name"]:
            results_item["di_name"] = "接点入力" + str(di_number)
        else:
            results_item["di_name"] = di_info["di_name"]

        # 「接点入力状態名称・接点入力状態アイコン」は現状態TBLの「接点入力{接点入力端子番号}_現状態」に対応する値を設定
        if state_info[f"di{di_number}_state"] == 0:
            results_item["di_state_name"] = di_info["di_on_name"]
            results_item["di_state_icon"] = di_info["di_on_icon"]
        else:
            results_item["di_state_name"] = di_info["di_off_name"]
            results_item["di_state_icon"] = di_info["di_off_icon"]

    # グループ名を設定
    results_item["group_name_list"] = group_name_list
    results_item["do_flag"] = do_info.get("do_flag")

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


def keyword_detection_device_list(detect_condition, keyword, device_info_list, group_info_list, device_group_relation):

    if detect_condition == 0:
        filtered_device_list = device_detect_all(keyword, device_info_list, group_info_list, device_group_relation)
    elif detect_condition == 1 or detect_condition == 2 or detect_condition == 3 or detect_condition == 4 or detect_condition == 5 or detect_condition == 6:
        filtered_device_list = device_detect(detect_condition, keyword, device_info_list, group_info_list, device_group_relation)
    else:
        filtered_device_list = device_info_list
    
    return filtered_device_list

# デバイス検索
def device_detect(detect_condition, keyword, device_info_list, group_info_list, device_group_relation):

    # AND,OR区切りでリスト化
    if " OR " in keyword:
        key_list = re.split(" OR ",keyword)
        logger.info(f"key_list:{key_list}")
        case = 2
    elif " AND " in keyword or " " in keyword or "\u3000" in keyword:
        key_list = re.split(" AND | |\u3000",keyword)
        logger.info(f"key_list:{key_list}")
        case = 1
    elif "-" == keyword[0] and keyword != "-":
        case = 3
    else:
        case = 0

    return_list = []

    for device_info in device_info_list:
        
        hit_list = []

        if detect_condition == 1:
            device_value = (
                device_info.get("device_data").get("config").get("device_name")
                if device_info.get("device_data").get("config").get("device_name")
                else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{device_info.get('imei')}(IMEI)"
            )
        elif detect_condition == 2:
            device_value = device_info.get("identification_id")
        elif detect_condition == 3:
            device_value = device_info.get("device_data").get("param").get("device_code")
        elif detect_condition == 4:
            device_id = device_info["device_id"]
            device_value = next((item["group_list"] for item in device_group_relation if item.get("device_id") == device_id), [])
            if device_value == []:
                continue
        elif detect_condition == 5:
            filtered_device_group_relation = next(
                (group for group in device_group_relation if group["device_id"] == device_info["device_id"]), {}
            ).get("group_list", [])
            device_value = [
                next((group for group in group_info_list if group["group_id"] == item2), {})
                .get("group_data", {})
                .get("config", {})
                .get("group_name", "")
                for item2 in filtered_device_group_relation
            ]
            if device_value == []:
                continue
        elif detect_condition == 6:
            device_id = device_info["device_id"]
            device_value = device_info.get("device_data").get("config").get("terminal_settings").get("do_info").get("do_name")
        else :
            pass

        # device_valueは各デバイスの検索評価対象の値
        logger.info(f"検索評価対象の値:{device_value}")

        #検索対象がNoneの場合,Not検索以外は該当データなし。Not検索の場合はすべて該当。
        if not device_value:
            device_value = ""
        
        if case == 1:
            # グループID、コントロール名検索の場合は、device_valueはリスト
            if isinstance(device_value, list):
                for value in device_value:
                    for key in key_list:
                        if key in value:
                            hit_list.append(1)
                        else:
                            hit_list.append(0)
                    logger.info(f"hit_list:{hit_list}")
                    if len(hit_list)!=0:
                        result = reduce(lambda x, y: x * y, hit_list)
                        if result == 1:
                            return_list.append(device_info)
                            break
            else:
                for key in key_list:
                    if key in device_value:
                        hit_list.append(1)
                    else:
                        hit_list.append(0)
                logger.info(f"hit_list:{hit_list}")
                if len(hit_list)!=0:
                    result = reduce(lambda x, y: x * y, hit_list)
                    if result == 1:
                        return_list.append(device_info)
        elif case == 2:
            if isinstance(device_value, list):
                for value in device_value:
                    for key in key_list:
                        if key in value:
                            hit_list.append(1)
                        else:
                            hit_list.append(0)
                    logger.info(f"hit_list:{hit_list}")
                    if len(hit_list)!=0:
                        result = sum(hit_list)
                        if result != 0:
                            return_list.append(device_info)
                            break
            else:
                for key in key_list:
                    if key in device_value:
                        hit_list.append(1)
                    else:
                        hit_list.append(0)
                logger.info(f"hit_list:{hit_list}")
                if len(hit_list)!=0:
                    result = sum(hit_list)
                    if result != 0:
                        return_list.append(device_info)
        elif case == 3:
            if isinstance(device_value, list):
                for value in device_value:
                    if keyword[1:] in value:
                        hit_list.append(0)
                    else:
                        hit_list.append(1)
                    logger.info(f"hit_list:{hit_list}")
                if len(hit_list)!=0:
                    result = reduce(lambda x, y: x * y, hit_list)
                    if result == 1:
                        return_list.append(device_info)
            else:
                if keyword[1:] in device_value:
                    pass
                else:
                    return_list.append(device_info)
        else:
            if isinstance(device_value, list):
                for value in device_value:
                    if keyword in value:
                        return_list.append(device_info)
                        break
            else:
                if keyword in device_value:
                    return_list.append(device_info)

    return return_list


def device_detect_all(keyword, device_info_list, group_info_list, device_group_relation):

    # AND,OR区切りでリスト化
    if " OR " in keyword:
        key_list = re.split(" OR ",keyword)
        logger.info(f"key_list:{key_list}")
        case = 2
    elif " AND " in keyword or " " in keyword or "\u3000" in keyword:
        key_list = re.split(" AND | |\u3000",keyword)
        logger.info(f"key_list:{key_list}")
        case = 1
    elif "-" == keyword[0] and keyword != "-":
        case = 3
    else:
        case = 0

    return_list = []

    for device_info in device_info_list:
        
        hit_list = []

        device_name = (
            device_info.get("device_data").get("config").get("device_name")
            if device_info.get("device_data").get("config").get("device_name")
            else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{device_info.get('sigfox_id')}(タグID)"
            if device_info.get("device_type") == "UnaTag"
            else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{device_info.get('imei')}(IMEI)"
        )
        device_id = device_info.get("identification_id")
        device_code = device_info.get("device_data").get("param").get("device_code")
        do_name = device_info.get("device_data").get("config").get("terminal_settings").get("do_info").get("do_name")
        filtered_device_group_relation = next(
            (group for group in device_group_relation if group["device_id"] == device_info["device_id"]), {}
        ).get("group_list", [])
        group_name_list = [
            next((group for group in group_info_list if group["group_id"] == item2), {})
            .get("group_data", {})
            .get("config", {})
            .get("group_name", "")
            for item2 in filtered_device_group_relation
        ]


        #Noneの場合にエラーが起きることの回避のため
        if device_name is None:
            device_name = ""
        if device_id is None:
            device_id = ""
        if device_code is None:
            device_code = ""
        if not do_name:
            do_name = ""
        if not group_name_list:
            group_name_list = ""

        if case == 1:
            for key in key_list:
                if (key in device_name) or (key in device_id) or (key in device_code) or (key in do_name) or any(key in group_name for group_name in group_name_list):
                    hit_list.append(1)
                else:
                    hit_list.append(0)
            logger.info(f"hit_list:{hit_list}")
            if len(hit_list)!=0:
                result = reduce(lambda x, y: x * y, hit_list)
                if result == 1:
                    return_list.append(device_info)
        elif case == 2:
            for key in key_list:
                if (key in device_name) or (key in device_id) or (key in device_code) or (key in do_name) or any(key in group_name for group_name in group_name_list):
                    hit_list.append(1)
                else:
                    hit_list.append(0)
            logger.info(f"hit_list:{hit_list}")
            if len(hit_list)!=0:
                result = sum(hit_list)
                if result != 0:
                    return_list.append(device_info)
        elif case == 3:
            if (keyword[1:] in device_name) or (keyword[1:] in device_id) or (keyword[1:] in device_code) or (keyword[1:] in do_name) or any(keyword[1:] in group_name for group_name in group_name_list):
                pass
            else:
                return_list.append(device_info)
        else:
            if (keyword in device_name) or (keyword in device_id) or (keyword in device_code) or (keyword in do_name) or any(keyword in group_name for group_name in group_name_list):
                return_list.append(device_info)

    return return_list
