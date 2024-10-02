import json
import os
import boto3
import ddb
import validate
import re
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from boto3.dynamodb.conditions import Key
from functools import reduce

# layer
import auth
import db
import ssm
import convert

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
region_name = os.environ.get("AWS_REGION")

logger = Logger()


@auth.verify_login_user()
def lambda_handler(event, context, user_info):
    logger.info(region_name)
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        try:
            tables = {
                "user_table": dynamodb.Table(ssm.table_names["USER_TABLE"]),
                "device_table": dynamodb.Table(ssm.table_names["DEVICE_TABLE"]),
                "group_table": dynamodb.Table(ssm.table_names["GROUP_TABLE"]),
                "device_state_table": dynamodb.Table(ssm.table_names["STATE_TABLE"]),
                "account_table": dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"]),
                "contract_table": dynamodb.Table(ssm.table_names["CONTRACT_TABLE"]),
                "pre_register_table": dynamodb.Table(ssm.table_names["PRE_REGISTER_DEVICE_TABLE"]),
                "device_relation_table": dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"]),
            }
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        ##################
        # 1 入力情報チェック
        ##################
        validate_result = validate.validate(event, user_info, tables)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        user_id = user_info["user_id"]
        user_type = user_info["user_type"]
        contract_id = user_info["contract_id"]
        # logger.info(user_id,user_type,contract_id)
        logger.info(f"ユーザ情報:{user_info}")

        logger.info(f"権限:{user_type}")
        device_id_list = []
        ##################
        # 3 デバイスID一覧取得(権限が管理者・副管理者の場合)
        ##################
        if user_type == "admin" or user_type == "sub_admin":
            # 3.1 デバイスID一覧取得
            contract_info = db.get_contract_info(contract_id, tables["contract_table"])
            if not contract_info:
                res_body = {"message": "契約情報が存在しません。"}
                return {
                    "statusCode": 500,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
            device_id_list = contract_info.get("contract_data", {}).get("device_list", [])

        ##################
        # 2 デバイスID一覧取得(権限が作業者・参照者の場合)
        ##################
        elif user_type == "worker" or user_type == "referrer":
            # 2.1 適用デバイスID、グループID一覧取得
            device_id_list = db.get_user_relation_device_id_list(
                user_id, tables["device_relation_table"]
            )
        else:
            res_body = {"message": "不正なユーザです。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        logger.info(f"デバイスID:{device_id_list}")

        ##################
        # デバイス順序更新
        ##################
        # 順序取得
        device_order = user_info.get("user_data", {}).get("config", {}).get("device_order", [])
        logger.info(f"デバイス順序:{device_order}")
        # 順序比較
        device_order_update = device_order_comparison(device_order, device_id_list)
        # 順序更新
        if device_order_update or type(device_order_update) is list:
            logger.info("try device order update")
            logger.info(f"最新のデバイス順序:{device_order_update}")
            db.update_device_order(device_order_update, user_id, tables["user_table"])
            logger.info("tried device order update")
            device_order = device_order_update
        else:
            logger.info("passed device order update")

        ##################
        # グループ名一覧取得
        ##################
        # グループID取得
        device_group_relation, all_groups = (
            [],
            [],
        )  # デバイスID毎のグループID一覧、重複のないグループID一覧
        for device_id in device_id_list:
            group_id_list = db.get_device_relation_group_id_list(
                device_id, tables["device_relation_table"]
            )
            device_group_relation.append({"device_id": device_id, "group_list": group_id_list})
            all_groups += group_id_list
        all_groups = set(all_groups)
        logger.info(f"デバイスグループ関連:{device_group_relation}")
        logger.info(f"重複のないグループID一覧:{all_groups}")

        # グループ情報取得
        group_info_list = []
        for item in all_groups:
            group_info = db.get_group_info(item, tables["group_table"])
            if group_info:
                group_info_list.append(group_info)
            else:
                logger.info(f"group information does not exist:{item}")
        logger.info(f"グループ情報:{group_info_list}")
        if group_info_list:
            group_info_list = sorted(
                group_info_list, key=lambda x: x["group_data"]["config"]["group_name"]
            )

        ##################
        # 6 デバイス一覧生成
        ##################
        order = 1
        device_list, device_info_list, device_info_list_order = [], [], []
        device_info_list = ddb.get_device_info_by_contract_id(contract_id,tables["device_table"])
        device_info_list = db.insert_id_key_in_device_info_list(device_info_list)

        for device_item in device_order:
            for device_info_item in device_info_list:
                if device_info_item["device_id"] == device_item:
                    device_info_list_order.append(device_info_item)
                    break

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
            device_info_list_order_filtered = device_info_list_order
        elif detect_condition != None:
            device_info_list_order_filtered = keyword_detection_device_list(detect_condition,keyword,device_info_list_order,device_group_relation)
        else:
            res_body = {"message": "検索条件が設定されていません。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
            

        for device_info in device_info_list_order_filtered:
            group_name_list = []
            # デバイス情報取得
            #device_info = ddb.get_device_info(item1, tables["device_table"])
            #logger.info({"device_info": device_info})
            if device_info:
                pass
            elif not device_info:
                logger.info(f"device information does not exist:{device_info["device_id"]}")
                continue
            else:
                res_body = {
                    "message": "デバイスIDに「契約状態:初期受信待ち」「契約状態:使用可能」の機器が複数紐づいています",
                }
                return {
                    "statusCode": 500,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }

            # グループID参照
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
            logger.info(f"グループ名:{group_name_list}")
            # デバイス現状態取得
            logger.info(f"device_info_dev_id:{device_info["device_id"]}")
            device_state = db.get_device_state(device_info["device_id"], tables["device_state_table"])
            if not device_state:
                logger.info(f"device current status information does not exist:{device_info["device_id"]}")

            # 機器異常状態判定
            device_abnormality = 0
            if (
                device_state.get("device_abnormality")
                or device_state.get("parameter_abnormality")
                or device_state.get("fw_update_abnormality")
                or device_state.get("device_healthy_state")
            ):
                device_abnormality = 1

            logger.info(f"device_state:{device_state}")

            # 最終受信日時取得
            last_receiving_time = ""
            if device_state:
                pattern = re.compile(r".*update_datetime$")
                matching_keys = [key for key in device_state.keys() if pattern.match(key)]
                matching_values = {key: device_state[key] for key in matching_keys}
                last_receiving_time = max(matching_values.values())

            # 接点入力リスト
            di_list = []
            for di in device_info["device_data"]["config"]["terminal_settings"].get(
                "di_list", []
            ):
                di_no = di["di_no"]
                di_state_label = f"di{di_no}_state"
                di_state = device_state.get(di_state_label)
                di_healthy_state_label = f"di{di_no}_healthy_state"
                di_healthy_state = device_state.get(di_healthy_state_label, 0)
                di_list.append(
                    {
                        "di_no": di_no,
                        "di_state": di_state,
                        "di_unhealthy": di_healthy_state,
                        "di_name": di.get("di_name"),
                        "di_on_name": di.get("di_on_name"),
                        "di_off_name": di.get("di_off_name"),
                    }
                )

            # 接点出力リスト
            do_list = []
            for do in device_info["device_data"]["config"]["terminal_settings"].get(
                "do_list", []
            ):
                do_list.append(
                    {
                        "do_no": do.get("do_no"),
                        "do_name": do.get("do_name"),
                        "do_di_return": do.get("do_di_return"),
                        "do_control": do.get("do_control"),
                    }
                )

            # デバイス一覧生成
            device_list.append(
                {
                    "device_id": device_info["device_id"],
                    "device_name": device_info["device_data"]["config"].get(
                        "device_name"
                    ),
                    "device_imei": device_info["imei"],
                    "sigfox_id": device_info["sigfox_id"],
                    "device_type": device_info["device_type"],
                    "group_name_list": group_name_list,
                    "device_code": device_info["device_data"]["param"].get(
                        "device_code"
                    ),
                    "last_receiving_time": last_receiving_time,
                    "signal_status": device_state.get("signal_state", 0),
                    "device_order": order,
                    "di_list": di_list,
                    "do_list": do_list,
                    "battery_near_status": device_state.get("battery_near_state", 0),
                    "device_abnormality": device_abnormality,
                }
            )
            order += 1

        if user_type == "admin" or user_type == "sub_admin":
            ##################
            # 7 登録前デバイス情報取得
            ##################
            pre_reg_device_info = ddb.get_pre_reg_device_info(
                contract_id, tables["pre_register_table"]
            )

            if keyword == None or keyword == "":
                pass
            elif detect_condition != None:
                pre_reg_device_info = keyword_detection_device_list_for_unregistration_device(detect_condition,keyword,pre_reg_device_info,device_group_relation)
            
            ##################
            # 8 応答メッセージ生成
            ##################
            res_body = {
                "message": "",
                "device_list": device_list,
                "unregistered_device_list": pre_reg_device_info,
            }
        elif user_type == "worker" or user_type == "referrer":
            res_body = {"message": "", "device_list": device_list}

        logger.info(f"レスポンス:{res_body}")
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }


# 順序比較
def device_order_comparison(device_order, device_id_list):
    if set(device_order) == set(device_id_list):
        return False
    if set(device_order) - set(device_id_list):
        diff1 = list(set(device_order) - set(device_id_list))
        logger.info(f"diff1:{diff1}")
        device_order = [item for item in device_order if item not in diff1]
    if set(device_id_list) - set(device_order):
        diff2 = list(set(device_id_list) - set(device_order))
        logger.info(f"diff2:{diff2}")
        device_order = device_order + diff2
    logger.info(device_order)
    return device_order


def keyword_detection_device_list(detect_condition,keyword,device_info_list,device_group_relation):

    if detect_condition == 0:
        filtered_device_list = device_detect_all(keyword,device_info_list)
    elif detect_condition == 1 or detect_condition == 2 or detect_condition == 3 or detect_condition == 4:
        filtered_device_list = device_detect(detect_condition,keyword,device_info_list,device_group_relation)
    else:
        filtered_device_list = device_info_list
    
    return filtered_device_list

# デバイス検索
def device_detect(detect_condition,keyword,device_info_list,device_group_relation):

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
            device_value = device_info.get("device_data").get("config").get("device_name")
        elif detect_condition == 2:
            device_value = device_info.get("imei")
        elif detect_condition == 3:
            device_value = device_info.get("device_data").get("param").get("device_code")
        elif detect_condition == 4:
            device_id = device_info["device_id"]
            device_value = next((item["group_list"] for item in device_group_relation if item.get("device_id") == device_id), [])
            if device_value == []:
                continue
        else :
            pass

        # device_valueは各デバイスの検索評価対象の値
        logger.info(f"検索評価対象の値:{device_value}")

        #検索対象がNoneの場合は次のデバイスの処理に移行
        if device_value is None:
            continue
        
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


def device_detect_all(keyword,device_info_list):

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

        device_name = device_info.get("device_data").get("config").get("device_name")
        device_id = device_info.get("imei")
        device_code = device_info.get("device_data").get("param").get("device_code")

        #Noneの場合にエラーが起きることの回避のため
        if device_name is None:
            device_name = ""
        if device_id is None:
            device_id = ""
        if device_code is None:
            device_code = ""

        if case == 1:
            for key in key_list:
                if (key in device_name) or (key in device_id) or (key in device_code):
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
                if (key in device_name) or (key in device_id) or (key in device_code):
                    hit_list.append(1)
                else:
                    hit_list.append(0)
            logger.info(f"hit_list:{hit_list}")
            if len(hit_list)!=0:
                result = sum(hit_list)
                if result != 0:
                    return_list.append(device_info)
        elif case == 3:
            if (keyword[1:] in device_name) or (keyword[1:] in device_id) or (keyword[1:] in device_code):
                pass
            else:
                return_list.append(device_info)
        else:
            if (keyword in device_name) or (keyword in device_id) or (keyword in device_code):
                return_list.append(device_info)

    return return_list


# 未登録デバイス検索
def keyword_detection_device_list_for_unregistration_device(detect_condition,keyword,device_info_list,device_group_relation):

    if detect_condition == 0:
        filtered_device_list = device_detect_all_for_unregistrated_device(keyword,device_info_list)
    elif detect_condition == 1 or detect_condition == 2 or detect_condition == 3 or detect_condition == 4:
        filtered_device_list = device_detect_for_unregistrated_device(detect_condition,keyword,device_info_list,device_group_relation)
    else:
        filtered_device_list = device_info_list
    
    return filtered_device_list

def device_detect_for_unregistrated_device(detect_condition,keyword,device_info_list,device_group_relation):

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

    #未登録デバイスにはデバイス名がないため、detect_condition = 1が来た時点で[]を返す
    if detect_condition == 1:
        return return_list

    for device_info in device_info_list:
        
        hit_list = []

        if detect_condition == 2:
            if device_info.get("device_code") == "MS-C0130":
                device_value = device_info.get("sigfox_id")
            else:
                device_value = device_info.get("device_imei")
        elif detect_condition == 3:
            device_value = device_info.get("device_code")
        elif detect_condition == 4:
            device_id = device_info["device_id"]
            device_value = next((item["group_list"] for item in device_group_relation if item.get("device_id") == device_id), [])
            if device_value == []:
                continue
        else :
            pass

        # device_valueは各デバイスの検索評価対象の値
        logger.info(f"検索評価対象の値:{device_value}")

        #検索対象がNoneの場合は次のデバイスの処理に移行
        if device_value is None:
            continue
        
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

def device_detect_all_for_unregistrated_device(keyword,device_info_list):

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

        if device_info.get("device_code") == "MS-C0130":
            device_id = device_info.get("sigfox_id")
        else:
            device_id = device_info.get("device_imei")
        device_code = device_info.get("device_code")

        #Noneの場合にエラーが起きることの回避のため
        if device_id is None:
            device_id = ""
        if device_code is None:
            device_code = ""

        if case == 1:
            for key in key_list:
                if (key in device_id) or (key in device_code):
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
                if (key in device_id) or (key in device_code):
                    hit_list.append(1)
                else:
                    hit_list.append(0)
            logger.info(f"hit_list:{hit_list}")
            if len(hit_list)!=0:
                result = sum(hit_list)
                if result != 0:
                    return_list.append(device_info)
        elif case == 3:
            if (keyword[1:] in device_id) or (keyword[1:] in device_code):
                pass
            else:
                return_list.append(device_info)
        else:
            if (keyword in device_id) or (keyword in device_code):
                return_list.append(device_info)

    return return_list
