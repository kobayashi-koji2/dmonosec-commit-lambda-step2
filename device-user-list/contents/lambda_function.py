import json
import os
import re
import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from functools import reduce

import auth
import db
import ssm

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
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
    except KeyError as e:
        body = {"message": e}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }

    try:
        pathParam = event.get("pathParameters", {})
        if not pathParam or "device_id" not in pathParam:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "パラメータが不正です。"}, ensure_ascii=False),
            }
        device_id = pathParam["device_id"]

        detect_condition = None
        keyword = None
        query_params = event.get("queryStringParameters")
        if query_params:
            keyword = query_params.get("keyword")
            query_param_detect_condition = query_params.get("detect_condition")
            if query_param_detect_condition:
                if query_param_detect_condition.isdecimal():
                    detect_condition = int(query_param_detect_condition)

        # ユーザー権限チェック
        if user.get("user_type") != "admin" and user.get("user_type") != "sub_admin":
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        # デバイスIDが契約に紐づいているかチェック
        contract = db.get_contract_info(user.get("contract_id"), contract_table)
        contract_device_list = contract.get("contract_data", {}).get("device_list", {})
        if device_id not in contract_device_list:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "不正なデバイスIDが指定されています。"}, ensure_ascii=False),
            }

        # ユーザー一覧生成
        user_list = []
        admin_user_id_list = db.get_admin_user_id_list(user.get("contract_id"), user_table)
        logger.debug(f"admin_user_id_list: {admin_user_id_list}")
        worker_user_id_list = db.get_device_relation_user_id_list(device_id, device_relation_table)
        logger.debug(f"worker_user_id_list: {worker_user_id_list}")
        user_id_list = admin_user_id_list + worker_user_id_list
        logger.debug(f"user_id_list: {user_id_list}")
        for user_id in user_id_list:
            logger.debug(user_id)
            user_info = db.get_user_info_by_user_id(user_id, user_table)
            account_info = db.get_account_info_by_account_id(
                user_info.get("account_id"), account_table
            )

            if keyword == None or keyword == "":
                get_flag = True
            elif detect_condition != None:
                get_flag = keyword_detection_user(detect_condition, keyword, account_info)
            else:
                res_body = {"message": "検索条件が設定されていません。"}
                return {
                    "statusCode": 400,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }

            if get_flag:
                user_list.append(
                    {
                        "user_id": user_id,
                        "user_name": account_info.get("user_data", {}).get("config", {}).get("user_name", ""),
                        "email_address": account_info.get("email_address", ""),
                    }
                )

        res_body = {
            "message": "",
            "user_list": user_list,
        }
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


def keyword_detection_user(detect_condition, keyword, account):

    account_config = account.get("user_data", {}).get("config", {})
    if detect_condition == 0:
        get_flag = user_detect_all(keyword, account_config, account)
    elif detect_condition == 1 or detect_condition == 2:
        get_flag = user_detect(detect_condition, keyword, account_config, account)
    else:
        get_flag = True
    
    return get_flag


def user_detect(detect_condition, keyword, account):

    account_config = account.get("user_data", {}).get("config", {})
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

    hit_list = []

    get_flag = False

    if detect_condition == 1:
        search_value = (
            account_config.get("user_name")
            if account_config.get("user_name")
            else account.get("email_address")
        )
        logger.info(f"user_name:{search_value}")
    elif detect_condition == 1:
        search_value = account.get("email_address")
        logger.info(f"mail_address:{search_value}")
    else :
        pass
        
    if case == 1:
        for key in key_list:
            if key in search_value:
                hit_list.append(1)
            else:
                hit_list.append(0)
        logger.info(f"hit_list:{hit_list}")
        if len(hit_list)!=0:
            result = reduce(lambda x, y: x * y, hit_list)
            if result == 1:
                get_flag = True
    elif case == 2:
        for key in key_list:
            if key in search_value:
                hit_list.append(1)
            else:
                hit_list.append(0)
        logger.info(f"hit_list:{hit_list}")
        if len(hit_list)!=0:
            result = sum(hit_list)
            if result != 0:
                get_flag = True
    elif case == 3:
        if keyword[1:] in search_value:
            pass
        else:
            get_flag = True
    else:
        if keyword in search_value:
            get_flag = True

    return get_flag


def user_detect_all(keyword, account):

    account_config = account.get("user_data", {}).get("config", {})
    get_flag = False
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
        
    hit_list = []

    user_name = (
        account_config.get("user_name")
        if account_config.get("user_name")
        else account.get("email_address")
    )
    email_address = account.get("email_address")

    #Noneの場合にエラーが起きることの回避のため
    if user_name is None:
        user_name = ""
    if email_address is None:
        email_address = ""

    if case == 1:
        for key in key_list:
            if (key in user_name) or (key in email_address):
                get_flag = True
        logger.info(f"hit_list:{hit_list}")
        if len(hit_list)!=0:
            result = reduce(lambda x, y: x * y, hit_list)
            if result == 1:
                get_flag = True
    elif case == 2:
        for key in key_list:
            if (key in user_name) or (key in email_address):
                hit_list.append(1)
            else:
                hit_list.append(0)
        logger.info(f"hit_list:{hit_list}")
        if len(hit_list)!=0:
            result = sum(hit_list)
            if result != 0:
                get_flag = True
    elif case == 3:
        if (keyword[1:] in user_name) or (keyword[1:] in email_address):
            pass
        else:
            get_flag = True
    else:
        if (keyword in user_name) or (keyword in email_address):
            get_flag = True

    return get_flag
