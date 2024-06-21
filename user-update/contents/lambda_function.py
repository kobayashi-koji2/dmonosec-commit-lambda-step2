import json
import os
import textwrap

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from botocore.exceptions import ClientError

import auth
import ssm
import convert
import db
import mail
import ddb
import validate

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()

web_domain = os.environ.get("WEB_DOMAIN")


def send_invite_email(email_address):
    mail_subject = "【モノセコム】初回ログイン通知"
    mail_body = f"""\
        以下URLへアクセスし、ログイン情報を入力し、パスワードを設定してください。
        ログイン情報は別途メールにて通知いたします。
        https://{web_domain}/
    """
    mail.send_email([email_address], mail_subject, textwrap.dedent(mail_body))


@auth.verify_login_user()
def lambda_handler(event, context, user):
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
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
        except KeyError as e:
            body = {"message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }

        # パラメータチェック
        validate_result = validate.validate(event, user, account_table, contract_table, user_table)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }

        # ユーザ新規登録
        if event["httpMethod"] == "POST":
            result = ddb.create_user_info(
                validate_result["request_params"],
                user["contract_id"],
                validate_result["contract_info"],
                account_table,
                ssm.table_names["ACCOUNT_TABLE"],
                ssm.table_names["USER_TABLE"],
                ssm.table_names["CONTRACT_TABLE"],
                ssm.table_names["DEVICE_RELATION_TABLE"],
            )
            # 招待メール送信
            send_invite_email(validate_result["request_params"]["email_address"])

        # ユーザ更新
        elif event["httpMethod"] == "PUT":
            result = ddb.update_user_info(
                validate_result["request_params"],
                account_table,
                user_table,
                device_relation_table,
                device_table,
                contract_table,
                ssm.table_names["ACCOUNT_TABLE"],
                ssm.table_names["USER_TABLE"],
                ssm.table_names["DEVICE_RELATION_TABLE"],
                ssm.table_names["DEVICE_TABLE"],
            )

        if not result[0]:
            logger.info(result[0])
            res_body = {"message": "ユーザの登録・更新に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(
                    res_body, ensure_ascii=False, default=convert.decimal_default_proc
                ),
            }

        # レスポンス用データ取得
        user_id = result[1]
        registed_user = db.get_user_info_by_user_id(user_id, user_table)
        registed_account = db.get_account_info_by_account_id(
            registed_user["account_id"], account_table
        )

        group_id_list = db.get_user_relation_group_id_list(user_id, device_relation_table)
        group_list = []
        for group_id in group_id_list:
            logger.info(group_id)
            group_info = db.get_group_info(group_id, group_table)
            logger.info(group_info)
            group_list.append(
                {
                    "group_id": group_id,
                    "group_name": group_info.get("group_data", {})
                    .get("config", {})
                    .get("group_name"),
                }
            )
        if group_list:
            group_list = sorted(group_list, key=lambda x:x['group_name'])

        device_id_list = db.get_user_relation_device_id_list(
            user_id, device_relation_table, include_group_relation=False
        )
        device_list = []
        for device_id in device_id_list:
            device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
            device_list.append(
                {
                    "device_id": device_id,
                    "device_name": device_info.get("device_data", {})
                    .get("config", {})
                    .get("device_name"),
                }
            )

        # レスポンス作成
        registed_account_cofig = registed_account.get("user_data", {}).get("config", {})
        res_body = {
            "message": "",
            "user_id": user_id,
            "email_address": registed_account.get("email_address"),
            "user_name": registed_account_cofig.get("user_name"),
            "user_type": registed_user.get("user_type"),
            "auth_status": registed_account_cofig.get("auth_status"),
            "mfa_flag": registed_account_cofig.get("mfa_flag"),
            "management_group_list": group_list,
            "management_device_list": device_list,
        }
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
