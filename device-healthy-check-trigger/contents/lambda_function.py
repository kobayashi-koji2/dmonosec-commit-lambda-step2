import os
import boto3
import ddb
import json
import ssm
import traceback
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
DEVICE_HEALTHY_CHECK_LAMBDA_NAME = os.environ["DEVICE_HEALTHY_CHECK_LAMBDA_NAME"]

logger = Logger()


def lambda_handler(event, context):
    logger.debug(f"lambda_handler開始 event={event}")

    try:
        # DynamoDB操作オブジェクト生成
        try:
            contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        except KeyError as e:
            logger.error("KeyError")
            return -1

        # 契約ID取得
        contract_id_list = ddb.get_contract_id_list(contract_table)
        if len(contract_id_list) == 0:
            logger.debug(f"契約データなし")
            return 0

        # デバイスヘルシーチェック処理呼び出し
        for contract_id in contract_id_list:
            # パラメータ設定
            input_event = {"event_trigger": "lambda-device-healthy-check-trigger", "contract_id": contract_id}
            Payload = json.dumps(input_event)

            # 呼び出し
            boto3.client("lambda").invoke(
                FunctionName=DEVICE_HEALTHY_CHECK_LAMBDA_NAME, InvocationType="Event", Payload=Payload
            )

        logger.debug("lambda_handler正常終了")
        return 0

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        return -1
