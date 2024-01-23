import json
import os

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

import auth
import db
import mail
import ssm
import validate


logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


@auth.verify_login_user
@validate.validate_parameter
def lambda_handler(event, context, login_user, user_id):
    res_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    }
    logger.info({"login_user": login_user})
    logger.info({"user_id": user_id})

    # DynamoDB操作オブジェクト生成
    try:
        account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
    except KeyError as e:
        body = {"message": e}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }

    try:
        # 権限チェック
        if login_user["user_type"] not in ["admin", "sub_admin"]:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "権限がありません。"}, ensure_ascii=False),
            }

        contract = db.get_contract_info(login_user["contract_id"], contract_table)
        logger.info({"contract": contract})

        if user_id not in contract.get("contract_data", {}).get("user_list", []):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "ユーザーに対しての操作権限がありません。"}, ensure_ascii=False),
            }

        # 通知対象ユーザーの存在チェック
        user = db.get_user_info_by_user_id(user_id, user_table)
        account = db.get_account_info(user_id, account_table)
        logger.info({"user": user})

        if not user or not account:
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps({"message": "ユーザーが存在しません。"}, ensure_ascii=False),
            }

        # ユーザーのデバイスIDリストを取得
        if user["user_type"] in ["worker", "referrer"]:
            device_id_list = db.get_user_relation_device_id_list(user_id, device_relation_table)
        elif user["user_type"] == "sub_admin":
            device_id_list = contract.get("contract_data", {}).get("device_list", [])
        else:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "指定のユーザーは通知対象ではありません。"}, ensure_ascii=False),
            }
        device_id_list = list(set(device_id_list))
        logger.info({"device_id_list": device_id_list})

        # 通知メールの本文作成
        device_name_list = []
        for device_id in device_id_list:
            device = db.get_device_info(device_id, device_table)
            device_name = device["device_data"].get("config", {}).get("device_name")
            if not device_name:
                device_name = "デバイス名設定なし"
            iccid = device["device_data"]["param"]["iccid"]
            device_name_list.append(f"{device_name}({iccid})")

        user_name = account["user_data"]["config"]["user_name"]
        email_address = account["email_address"]

        mail_body = (
            "お客様の登録情報を通知いたします。\n"
            f"ユーザー名：{user_name}\n"
            f"メールアドレス：{email_address}\n"
            "\n"
            "管理対象デバイス：\n"
        ) + "\n".join(device_name_list)

        subject = "【モノセコムWEB】お客様情報通知"

        # 通知メール送信
        try:
            mail.send_email([email_address], subject, mail_body)
        except ClientError:
            logger.error("メール送信エラー", exc_info=True)
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps({"message": "メール送信に失敗しました。"}, ensure_ascii=False),
            }

        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps({"message": ""}, ensure_ascii=False),
        }

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
