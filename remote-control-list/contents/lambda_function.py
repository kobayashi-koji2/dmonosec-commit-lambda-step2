import os
import json
import traceback
from decimal import Decimal

from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
import boto3

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
            cotract_id = user_info["contract_id"]
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

        ### 5. 遠隔制御一覧生成
        results = list()
        # デバイス情報取得
        for device_id in device_id_list:
            device_info = ddb.get_device_info_only_pj2(device_id, device_table)
            logger.debug({"device_info": device_info})

            if device_info is not None:
                # 現状態情報取得
                state_info = db.get_device_state(device_id, device_state_table)
                # 現状態情報がない場合は次のデバイスへ
                if not state_info:
                    continue
                logger.debug(f"state_info: {state_info}")
                device_imei = device_info["imei"]
                device_name = (
                    device_info.get("device_data", {}).get("config", {}).get("device_name", "")
                )
                # 接点出力一覧
                do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
                # 接点入力一覧
                di_list = device_info["device_data"]["config"]["terminal_settings"]["di_list"]

                # グループ情報取得
                filtered_device_group_relation = next(
                    (group for group in device_group_relation if group["device_id"] == device_id), {}
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
                for do_info in do_list:
                    if not do_info["do_control"]:
                        continue
                    res_item = __generate_response_items(
                        device_id, device_name, device_imei, do_info, di_list, state_info, group_name_list
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


def __generate_response_items(device_id, device_name, device_imei, do_info, di_list, state_info, group_name_list):
    results_item = dict()
    results_item["device_id"] = device_id
    results_item["device_name"] = device_name
    results_item["device_imei"] = device_imei
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
