import json
import os
import re
import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from collections import OrderedDict
from functools import reduce

import auth
import db
import ddb
import ssm
import convert

patch_all()

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user()
def lambda_handler(event, context, user):
    res_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }
    logger.info({"user": user})

    # DynamoDB操作オブジェクト生成
    try:
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
    except KeyError as e:
        body = {"message": e}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }

    try:
        if user.get("user_type") != "admin" and user.get("user_type") != "sub_admin":
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        contract = db.get_contract_info(user.get("contract_id"), contract_table)
        logger.debug(contract)
        device_id_list = contract.get("contract_data", {}).get("device_list", {})
        logger.debug(device_id_list)

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

        device_list, device_info_list = [], []
        device_info_list = ddb.get_device_info_by_contract_id(user.get("contract_id"), device_table)
        device_info_list = db.insert_id_key_in_device_info_list(device_info_list)

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

        for device_info in device_info_list_filtered:
            if device_info:
                pass
            elif not device_info:
                logger.info(f"device information does not exist:{device_info["device_id"]}")
                continue
            notification_settings = (
                device_info.get("device_data", {}).get("config", {}).get("notification_settings", {})
            )
            if not notification_settings:
                continue

            group_name_list = []
            filtered_device_group_relation = next(
                (group for group in device_group_relation if group["device_id"] == device_info["device_id"]), {}
            ).get("group_list", [])
            logger.info(f"グループID参照:{filtered_device_group_relation}")
            # グループ名参照
            for item2 in filtered_device_group_relation:
                group_name_list.append(
                    next((group for group in group_info_list if group["group_id"] == item2), {})
                    .get("group_data", {})
                    .get("config", {})
                    .get("group_name", "")
                )
            if group_name_list:
                group_name_list.sort()

            device_list.append(
                {
                    "device_id": device_id,
                    "device_name": device_info.get("device_data", {}).get("config", {}).get("device_name", ""),
                    "group_name_list": group_name_list,
                    "device_imei": device_info.get("imei", ""),
                    "device_sigfox_id": device_info.get("sigfox_id", ""),
                    "device_code": device_info.get("device_data", {}).get("param", {}).get("device_code", "")
                }
            )

        res_body = {
            "message": "",
            "device_list": device_list,
        }
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }


def keyword_detection_device_list(detect_condition, keyword, device_info_list, group_info_list, device_group_relation):

    if detect_condition == 0:
        filtered_device_list = device_detect_all(keyword, device_info_list, group_info_list, device_group_relation)
    elif detect_condition == 1 or detect_condition == 2 or detect_condition == 3 or detect_condition == 4:
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
    elif "-" == keyword[0]:
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
                else f"【{device_info.get("device_data", {}).get("param", {}).get("device_code")}】{device_info.get('sigfox_id')}(タグID)"
                if device_info.get("device_type") == "UnaTag"
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
        else :
            pass

        # device_valueは各デバイスの検索評価対象の値
        logger.info(f"検索評価対象の値:{device_value}")

        #検索対象がNoneの場合,Not検索以外は該当データなし。Not検索の場合はすべて該当。
        if not device_value:
            device_value = ""
        
        if case == 1:
            # グループID検索の場合は、device_valueはリスト
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
    elif "-" == keyword[0]:
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
        if not group_name_list:
            group_name_list = ""

        if case == 1:
            for key in key_list:
                if (key in device_name) or (key in device_id) or (key in device_code) or (key in group_name_list):
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
                if (key in device_name) or (key in device_id) or (key in device_code) or (key in group_name_list):
                    hit_list.append(1)
                else:
                    hit_list.append(0)
            logger.info(f"hit_list:{hit_list}")
            if len(hit_list)!=0:
                result = sum(hit_list)
                if result != 0:
                    return_list.append(device_info)
        elif case == 3:
            if (keyword[1:] in device_name) or (keyword[1:] in device_id) or (keyword[1:] in device_code) or (keyword[1:] in group_name_list):
                pass
            else:
                return_list.append(device_info)
        else:
            if (keyword in device_name) or (keyword in device_id) or (keyword in device_code) or (keyword in group_name_list):
                return_list.append(device_info)

    return return_list
