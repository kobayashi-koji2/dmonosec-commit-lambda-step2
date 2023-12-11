import json
import os
import boto3
import logging
import validate
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

# layer
import db
import ssm
import convert

# テスト
import db_dev

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

parameter = None
logger = logging.getLogger()


def lambda_handler(event, context):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # コールドスタートの場合パラメータストアから値を取得してグローバル変数にキャッシュ
        global parameter
        if not parameter:
            print("try ssm get parameter")
            response = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(response)
            print("tried ssm get parameter")
        else:
            print("passed ssm get parameter")
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(parameter["USER_TABLE"])
            device_table = dynamodb.Table(parameter.get("DEVICE_TABLE"))
            device_state_table = dynamodb.Table(parameter.get("STATE_TABLE"))
            account_table = dynamodb.Table(parameter.get("ACCOUNT_TABLE"))
            contract_table = dynamodb.Table(parameter.get("CONTRACT_TABLE"))
            pre_register_table = dynamodb.Table(
                parameter.get("PRE_REGISTER_DEVICE_TABLE")
            )
            user_device_group_table = dynamodb.Table(
                parameter.get("USER_DEVICE_GROUP_TABLE")
            )
        except KeyError as e:
            parameter = None
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        validate_result = validate.validate(event, user_table)
        if validate_result["code"] != "0000":
            return {
                "statusCode": 200,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        user_info = validate_result["user_info"]
        decoded_idtoken = validate_result["decoded_idtoken"]
        user_id = user_info["Item"]["user_id"]
        user_type = user_info["Item"]["user_type"]
        # contract_id = decoded_idtoken['contract_id']
        contract_id = "na1234567"

        ##################
        # デバイス順序更新
        ##################
        # 更新前デバイス順序
        device_order_list = user_info["Item"]["user_data"]["device_order"]
        # 管理デバイス(直接)
        # manage_device_list = db_dev.get_user_device_group_table('u-'+user_id, user_device_group_table, sk_prefix='d-')
        # print(user_id)
        # print(manage_device_list)
        # manage_group_list = db_dev.get_user_device_group_table(user_id, user_device_group_table, sk_prefix='g-').get('key2',[])
        # 管理デバイス(グループ)
        # group_manage_device_list = []
        # or item in manage_group_list:
        # group_manage_device_list.append(db_dev.get_user_device_group_table(user_id, user_device_group_table, sk_prefix='g-').get('key2',[])
        # group_manage_devices = db_dev.get_user
        # print(manage_device_list)
        # 更新判定

        # 更新後デバイス順序
        # device_order_list = db.get_device_order(manage_device_list,user_id,device_order_list_old)

        ##################
        # デバイス一覧取得
        ##################
        device_list = get_device_list(
            device_order_list, device_table, device_state_table
        )

        try:
            if user_type == "admin" or user_type == "sub_admin":
                # アカウントに紐づくデバイスIDを取得
                # device_id_list = db.get_contract_info(contract_id,contract_table)
                # デバイス情報取得
                # device_info = get_device_list(device_order_list, device_table, device_state_table)
                # 未登録デバイス情報取得
                pre_reg_device_info = db.get_pre_reg_device_info(
                    contract_id, pre_register_table
                )
                res_body = {
                    "code": "0000",
                    "message": "",
                    "device_list": device_list,
                    "unregistered_device_list": pre_reg_device_info,
                }
            elif user_type == "worker" or user_type == "referrer":
                logger.debug("権限:作業者")
                # デバイス情報取得
                # device_info = get_device_list(device_order_list, device_table, device_state_table)
                res_body = {"code": "0000", "messege": "", "device_list": device_list}
            else:
                res_body = {"code": "9999", "messege": "ユーザ権限が不正です。"}
        except ClientError as e:
            print(e)
            body = {"code": "9999", "message": "デバイス一覧の取得に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        print(f"レスポンスボディ:{res_body}")
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(
                res_body, ensure_ascii=False, default=convert.decimal_default_proc
            ),
        }
    except Exception as e:
        print(e)
        body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }


# デバイス一覧取得
def get_device_list(device_list, device_table, device_state_table):
    device_state_list, not_device_info_list, not_state_info_list = [], [], []
    order = 0
    for item in device_list:
        order += 1
        device_info = db.get_device_info(item, device_table)
        # デバイス現状態取得
        device_state = db.get_device_state(item, device_state_table)
        if len(device_info["Items"]) == 0:
            not_device_info_list.append(item)
            continue
        elif "Item" not in device_state:
            not_state_info_list.append(item)
            # レスポンス生成(現状態情報なし)
            device_state_list.append(
                {
                    "device_id": item,
                    "device_name": device_info["Items"][0]["device_data"]["config"][
                        "device_name"
                    ],
                    "imei": device_info["Items"][0]["imei"],
                    "device_order": order,
                    "signal_status": "",
                    "battery_near_status": "",
                    "device_abnormality": "",
                    "parameter_abnormality": "",
                    "fw_update_abnormality": "",
                    "device_unhealthy": "",
                    "di_unhealthy": "",
                }
            )
            continue
        # レスポンス生成(現状態情報あり)
        device_state_list.append(
            {
                "device_id": item,
                "device_name": device_info["Items"][0]["device_data"]["config"][
                    "device_name"
                ],
                "imei": device_info["Items"][0]["imei"],
                "device_order": order,
                "signal_status": device_state["Item"]["signal_status"],
                "battery_near_status": device_state["Item"]["battery_near_status"],
                "device_abnormality": device_state["Item"]["device_abnormality"],
                "parameter_abnormality": device_state["Item"]["parameter_abnormality"],
                "fw_update_abnormality": device_state["Item"]["fw_abnormality"],
                "device_unhealthy": "",
                "di_unhealthy": "",
            }
        )

    print("情報が存在しないデバイス:", not_device_info_list)
    print("現状態が存在しないデバイス:", not_state_info_list)
    return device_state_list
