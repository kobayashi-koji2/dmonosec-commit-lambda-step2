import json
import os
import time

import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr, Key
from aws_lambda_powertools import Logger

import auth
import db
import ddb
import ssm
import validate

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
cognito = boto3.client("cognito-idp")
COGNITO_USER_POOL_ID = os.environ["COGNITO_USER_POOL_ID"]


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
        device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
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

        # 削除対象ユーザーの存在チェック
        user = db.get_user_info_by_user_id(user_id, user_table)
        logger.info({"user": user})

        if not user:
            return {
                "statusCode": 404,
                "headers": res_headers,
                "body": json.dumps({"message": "ユーザーが存在しません。"}, ensure_ascii=False),
            }

        transact_items = []

        # 契約情報から削除対象のユーザーを削除
        contract_user_list = contract.get("contract_data").get("user_list", [])
        contract_user_list.remove(user_id)
        transact_items.append(
            {
                "Update": {
                    "TableName": contract_table.table_name,
                    "Key": {"contract_id": {"S": contract["contract_id"]}},
                    "UpdateExpression": "SET #contract_data.#user_list = :user_list",
                    "ExpressionAttributeNames": {
                        "#contract_data": "contract_data",
                        "#user_list": "user_list",
                    },
                    "ExpressionAttributeValues": {
                        ":user_list": {"L": [{"S": u} for u in contract_user_list]}
                    },
                }
            }
        )

        # 通知先から削除対象のユーザーを削除
        device_id_list = contract.get("contract_data").get("device_list", [])
        for device_id in device_id_list:
            device = db.get_device_info(device_id, device_table)
            if not device:
                continue
            logger.debug(device.get("device_id"))
            notification_settings = (
                device.get("device_data").get("config").get("notification_settings", {})
            )
            update = False
            for notification_setting in notification_settings:
                notification_target_list = notification_setting.get("notification_target_list", [])
                if user_id in notification_target_list:
                    notification_target_list.remove(user_id)
                    update = True
            if update:
                transact_items.append(
                    {
                        "Update": {
                            "TableName": device_table.table_name,
                            "Key": {"device_id": {"S": device["device_id"]}},
                            "UpdateExpression": "SET #device_data.#config.#notification_settings = :notification_settings",
                            "ExpressionAttributeNames": {
                                "#device_data": "device_data",
                                "#config": "config",
                                "#notification_settings": "notification_settings",
                            },
                            "ExpressionAttributeValues": {
                                ":notification_settings": notification_settings
                            },
                        }
                    }
                )

        # 削除対象のデバイス関係を取得して削除
        device_relation_list = db.get_device_relation(
            f"u-{user_id}", device_relation_table, sk_prefix="d-"
        ) + db.get_device_relation(f"u-{user_id}", device_relation_table, sk_prefix="g-")
        logger.info({"device_relation_list": device_relation_list})
        transact_items.extend(
            [
                {
                    "Delete": {
                        "TableName": device_relation_table.table_name,
                        "Key": {
                            "key1": {"S": device_relation["key1"]},
                            "key2": {"S": device_relation["key2"]},
                        },
                    }
                }
                for device_relation in device_relation_list
            ]
        )

        # モノセコムユーザー管理テーブルに削除日時を設定
        del_datetime = int(time.time() * 1000)
        logger.info({"del_datetime": del_datetime})
        transact_items.append(
            {
                "Update": {
                    "TableName": user_table.table_name,
                    "Key": {"user_id": {"S": user_id}},
                    "UpdateExpression": "SET #user_data.#config.#del_datetime = :del_datetime",
                    "ExpressionAttributeNames": {
                        "#user_data": "user_data",
                        "#config": "config",
                        "#del_datetime": "del_datetime",
                    },
                    "ExpressionAttributeValues": {":del_datetime": {"N": str(del_datetime)}},
                }
            }
        )

        # アカウント管理テーブルに削除日時を設定
        # モノセコムユーザー管理テーブルからアカウントIDが一致するレコードを取得
        user_list = user_table.query(
            IndexName="account_id_index",
            KeyConditionExpression=Key("account_id").eq(user["account_id"]),
            FilterExpression=Attr("user_id").ne(user["user_id"]),
        ).get("Items", [])

        # 他契約で使われていなければ、アカウント管理テーブルに削除日時を設定
        if not [
            x
            for x in user_list
            if x.get("user_data", {}).get("config", {}).get("del_datetime") is None
        ]:
            transact_items.append(
                {
                    "Update": {
                        "TableName": account_table.table_name,
                        "Key": {"account_id": {"S": user["account_id"]}},
                        "UpdateExpression": "SET #user_data.#config.#del_datetime = :del_datetime",
                        "ExpressionAttributeNames": {
                            "#user_data": "user_data",
                            "#config": "config",
                            "#del_datetime": "del_datetime",
                        },
                        "ExpressionAttributeValues": {":del_datetime": {"N": str(del_datetime)}},
                    }
                }
            )

        # DB更新
        if not db.execute_transact_write_item(transact_items):
            logger.error("ユーザー情報削除に失敗", exc_info=True)
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps({"message": "ユーザー情報の削除に失敗しました。"}, ensure_ascii=False),
            }

        # Cognitoのユーザープールからユーザー削除
        account = db.get_account_info_by_account_id(user["account_id"], account_table)
        try:
            cognito.admin_delete_user(UserPoolId=COGNITO_USER_POOL_ID, Username=account["auth_id"])
        except ClientError:
            logger.error("Cognitoのユーザー削除に失敗", exc_info=True)
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps({"message": "Cognitoのユーザー削除に失敗しました。"}, ensure_ascii=False),
            }

        return {"statusCode": 204, "headers": res_headers}

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
