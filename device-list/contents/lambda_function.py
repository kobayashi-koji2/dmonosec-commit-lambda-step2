import json
import os
import boto3
import ddb
import validate
import re
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

dynamodb = boto3.resource("dynamodb")
from boto3.dynamodb.conditions import Key

# layer
import db
import ssm
import convert

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
region_name = os.environ.get("AWS_REGION")

logger = Logger()


def lambda_handler(event, context):
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
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        ##################
        # 1 入力情報チェック
        ##################
        validate_result = validate.validate(event, tables)
        if validate_result["code"] != "0000":
            return {
                "statusCode": 200,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        user_info = validate_result["user_info"]["Item"]
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
            if "Item" not in contract_info:
                res_body = {"code": "9999", "message": "契約情報が存在しません。"}
                return {
                    "statusCode": 200,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }
            device_id_list = (
                contract_info.get("Item", {}).get("contract_data", {}).get("device_list", [])
            )

        ##################
        # 2 デバイスID一覧取得(権限が作業者・参照者の場合)
        ##################
        elif user_type == "worker" or user_type == "referrer":
            # 2.1 適用デバイスID、グループID一覧取得
            device_relation = db.get_device_relation(
                f"u-{user_id}", tables["device_relation_table"]
            )
            logger.info(device_relation)
            for item1 in device_relation:
                item1 = item1["key2"]
                # ユーザに紐づくデバイスIDを取得
                if item1.startswith("d-"):
                    device_id_list.append(re.sub("^d-", "", item1))
                # グループIDをキーにデバイスIDを取得
                elif item1.startswith("g-"):
                    device_group_relation = db.get_device_relation(
                        item1, tables["device_relation_table"], sk_prefix="d-"
                    )
                    for item2 in device_group_relation:
                        device_id_list.append(re.sub("^d-", "", item2["key2"]))
            # 2.2 デバイスID一覧生成
            device_id_list = set(device_id_list)
        else:
            res_body = {"code": "9999", "messege": "不正なユーザです。"}
            return {
                "statusCode": 200,
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
        if device_order_update:
            logger.info("try device order update")
            logger.info(f"最新のデバイス順序:{device_order_update}")
            res = db.update_device_order(device_order_update, user_id, tables["user_table"])
            logger.info("tried device order update")
            device_order = device_order_update
        else:
            logger.info("passed device order update")

        ##################
        # グループ名一覧取得
        ##################
        # グループID取得
        device_group_relation, all_groups = [], []  # デバイスID毎のグループID一覧、重複のないグループID一覧
        for item1 in device_id_list:
            trimmed_prefix = []  # 識別子を削除したグループID
            device_group_relation_res = db.get_device_relation(
                f"d-{item1}",
                tables["device_relation_table"],
                sk_prefix="g-",
                gsi_name="key2_index",
            )
            # グループIDの抽出とprefixのトリミング
            for item2 in device_group_relation_res:
                trimmed_prefix.append(re.sub("^g-", "", item2["key1"]))
            logger.info(f"グループ一覧:{trimmed_prefix}")
            device_group_relation.append({"device_id": item1, "group_list": trimmed_prefix})
            all_groups += trimmed_prefix
        all_groups = set(all_groups)
        logger.info(f"デバイスグループ関連:{device_group_relation}")
        logger.info(f"重複のないグループID一覧:{all_groups}")

        # グループ情報取得
        group_info_list = []
        for item in all_groups:
            group_info = db.get_group_info(item, tables["group_table"])
            if "Item" in group_info:
                group_info_list.append(group_info["Item"])
            else:
                logger.info(f"group information does not exist:{item}")
        logger.info(f"グループ情報:{group_info_list}")

        ##################
        # 6 デバイス一覧生成
        ##################
        order = 1
        device_list, device_info_list = [], []
        for item1 in device_order:
            group_name_list = []
            # デバイス情報取得
            device_info = ddb.get_device_info(item1, tables["device_table"])
            if len(device_info["Items"]) == 1:
                device_info_list.append(device_info["Items"][0])
            elif len(device_info["Items"]) == 0:
                logger.info(f"device information does not exist:{item1}")
                continue
            else:
                res_body = {
                    "code": "9999",
                    "message": "デバイスIDに「契約状態:初期受信待ち」「契約状態:使用可能」の機器が複数紐づいています",
                }
                return {
                    "statusCode": 500,
                    "headers": res_headers,
                    "body": json.dumps(res_body, ensure_ascii=False),
                }

            # グループID参照
            filtered_device_group_relation = next(
                (group for group in device_group_relation if group["device_id"] == item1), {}
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
            logger.info(f"グループ名:{group_name_list}")
            # デバイス現状態取得
            device_state = db.get_device_state(item1, tables["device_state_table"]).get("Item", {})
            if "Item" not in device_state:
                logger.info(f"device current status information does not exist:{item1}")

            # デバイス一覧生成
            device_list.append(
                {
                    "device_id": item1,
                    "device_name": device_info["Items"][0]["device_data"]["config"]["device_name"],
                    "device_imei": device_info["Items"][0]["imei"],
                    "group_name_list": group_name_list,
                    "device_order": order,
                    "signal_status": device_state.get("signal_status", ""),
                    "battery_near_status": device_state.get("battery_near_status", ""),
                    "device_abnormality": device_state.get("device_abnormality", ""),
                    "parameter_abnormality": device_state.get("parameter_abnormality", ""),
                    "fw_update_abnormality": device_state.get("fw_abnormality", ""),
                    "device_unhealthy": "",  # フェーズ2
                    "di_unhealthy": "",  # フェーズ2
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
            ##################
            # 8 応答メッセージ生成
            ##################
            res_body = {
                "code": "0000",
                "message": "",
                "device_list": device_list,
                "unregistered_device_list": pre_reg_device_info,
            }
        elif user_type == "worker" or user_type == "referrer":
            res_body = {"code": "0000", "message": "", "device_list": device_list}

        logger.info(f"レスポンス:{res_body}")
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
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
