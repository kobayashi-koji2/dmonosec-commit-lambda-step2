import json
import os
import boto3
import traceback
import re
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from aws_xray_sdk.core import patch_all
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
def lambda_handler(event, context, user):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(event, user)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        group_list = []
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

            contract_info = db.get_contract_info(user["contract_id"], contract_table)
            for group_id in contract_info.get("contract_data", {}).get("group_list", {}):
                group_info = db.get_group_info(group_id, group_table)
                unregistered_device_id_list = db.get_group_relation_pre_register_device_id_list(group_id, device_relation_table)
                if len(unregistered_device_id_list) >= 1:
                    unregistered_device_flag = 1
                else:
                    unregistered_device_flag = 0

                if keyword == None or keyword == "":
                    get_flag = True
                elif detect_condition != None:
                    get_flag = keyword_detection_group(detect_condition,keyword,group_info)
                else:
                    res_body = {"message": "検索条件が設定されていません。"}
                    return {
                        "statusCode": 400,
                        "headers": res_headers,
                        "body": json.dumps(res_body, ensure_ascii=False),
                    }
                
                if get_flag:
                    group_list.append(
                        {
                            "group_id": group_id,
                            "group_name": group_info.get("group_data", {})
                            .get("config", {})
                            .get("group_name", {}),
                            "unregistered_device_flag": unregistered_device_flag
                        }
                    )
                logger.info(group_list)
            if group_list:
                if query_params.get("unregistered_device_sort_flag") == 1:
                    group_list = sorted(group_list, key=lambda x:(x['unregistered_device_flag'] == 0, x['group_name']))
                else:
                    group_list = sorted(group_list, key=lambda x:x['group_name'])

        except ClientError as e:
            logger.info(e)
            logger.info(traceback.format_exc())
            body = {"message": "グループ一覧の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        res_body = {"message": "", "group_list": group_list}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
    
def keyword_detection_group(detect_condition,keyword,group_info):

    if detect_condition == 1:
        #group_nameで検索
        get_flag = group_detect(detect_condition,keyword,group_info)
    else:
        #無効なdetect_condition(検索条件)の場合は検索にかけない。
        get_flag = True
    
    return get_flag

def group_detect(detect_condition,keyword,group_info):

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

    get_flag = False

    if detect_condition == 1:
        device_value = group_info.get("group_data").get("config").get("group_name")
        logger.info(f"group_name:{device_value}")
    else :
        pass

    #グループ名がNoneの場合は取得しない
    if device_value is None:
        return False
        
    if case == 1:
        for key in key_list:
            if key in device_value:
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
            if key in device_value:
                hit_list.append(1)
            else:
                hit_list.append(0)
        logger.info(f"hit_list:{hit_list}")
        if len(hit_list)!=0:
            result = sum(hit_list)
            if result != 0:
                get_flag = True
    elif case == 3:
        if keyword[1:] in device_value:
            pass
        else:
            get_flag = True
    else:
        if keyword in device_value:
            get_flag = True

    return get_flag
