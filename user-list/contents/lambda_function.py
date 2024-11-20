import json
import os
import time
import re
import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from botocore.exceptions import ClientError
from functools import reduce

import auth
import ssm
import db
import convert

import validate

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


@auth.verify_login_user()
def lambda_handler(event, context, login_user):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(event, login_user)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        user_list = []
        try:
            detect_condition = None
            keyword = None
            query_params = event.get("queryStringParameters")
            if query_params:
                keyword = query_params.get("keyword")
                query_param_detect_condition = query_params.get("detect_condition")
                if query_param_detect_condition:
                    if query_param_detect_condition.isdecimal():
                        detect_condition = int(query_param_detect_condition)

            contract_info = db.get_contract_info(login_user["contract_id"], contract_table)
            for user_id in contract_info.get("contract_data", {}).get("user_list", {}):
                user = db.get_user_info_by_user_id(user_id, user_table)
                if user.get("user_type") == "admin":
                    continue

                account = db.get_account_info_by_account_id(user.get("account_id"), account_table)
                account_config = account.get("user_data", {}).get("config", {})

                auth_status = account_config.get("auth_status")
                if auth_status == "unauthenticated":
                    if account_config.get("auth_period", 0) / 1000 < int(time.time()):
                        auth_status = "expired"

                if keyword == None or keyword == "":
                    get_flag = True
                elif detect_condition != None:
                    get_flag = keyword_detection_user(detect_condition, keyword, account)
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
                            "user_id": user.get("user_id"),
                            "email_address": account.get("email_address"),
                            "user_name": account_config.get("user_name"),
                            "user_type": user.get("user_type"),
                            "auth_status": auth_status,
                        }
                    )
                logger.info(user_list)
        except ClientError as e:
            logger.info(e)
            body = {"message": "ユーザ一覧の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        res_body = {"message": "", "user_list": user_list}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }


def keyword_detection_user(detect_condition, keyword, account):

    account_config = account.get("user_data", {}).get("config", {})
    if detect_condition == 0:
        get_flag = user_detect_all(keyword, account)
    elif detect_condition == 1 or detect_condition == 2:
        get_flag = user_detect(detect_condition, keyword, account)
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
