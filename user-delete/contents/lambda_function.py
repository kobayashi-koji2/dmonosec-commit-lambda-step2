import json
import os
import time

import boto3
from botocore.exceptions import ClientError
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
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
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

        if user is None:
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps({"message": "ユーザーが存在しません。"}, ensure_ascii=False),
            }

        # 削除対象のデバイス関係を取得して削除
        device_relation_list = db.get_device_relation(
            f"u-{user_id}", device_relation_table, sk_prefix="d-"
        ) + db.get_device_relation(f"u-{user_id}", device_relation_table, sk_prefix="g-")
        logger.info({"device_relation_list": device_relation_list})

        transact_items = [
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

        if transact_items and not db.execute_transact_write_item(transact_items):
            logger.error("デバイス関係の削除に失敗")
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps({"message": "デバイス関係の削除に失敗しました。"}, ensure_ascii=False),
            }

        # Cognitoのユーザープールからユーザー削除
        try:
            cognito.admin_delete_user(UserPoolId=COGNITO_USER_POOL_ID, Username=user_id)
        except ClientError:
            logger.error("Cognitoのユーザー削除に失敗", exc_info=True)
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps({"message": "Cognitoのユーザー削除に失敗しました。"}, ensure_ascii=False),
            }

        # 削除日時設定
        del_datetime = int(time.time() * 1000)
        logger.info({"del_datetime": del_datetime})

        user_data = user.get("user_data", {})
        if "config" in user_data:
            user_data["config"]["del_datetime"] = del_datetime
        else:
            user_data["config"] = {"del_datetime": del_datetime}

        ddb.update_user_data(user_id, user_data, user_table)

        return {"statusCode": 204, "headers": res_headers}

    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
