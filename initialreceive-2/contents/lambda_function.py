import os
import ddb
import ssm
import json
import boto3
import traceback
from soracom_func import soracom_sim_terminate_api
from aws_lambda_powertools import Logger

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]

logger = Logger()


def get_params(event):
    params = {"iccid": None if "iccid" not in event else event["iccid"]}
    return params


def validate(params):
    if len(params["iccid"]) == 0:
        return 1
    return 0


def lambda_handler(event, context):
    try:
        # DynamoDB操作オブジェクト生成
        try:
            iccid_table = dynamodb.Table(ssm.table_names["ICCID_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            operator_table = dynamodb.Table(ssm.table_names["OPERATOR_TABLE"])
        except KeyError as e:
            logger.info("KeyError")
            return -1

        # eventからパラメータ取得
        params = get_params(event)
        logger.debug(f"params={params}")

        # バリデーションチェック
        validate_result = validate(params)
        if validate_result != 0:
            return -1

        # ICCID情報取得
        iccid_info = ddb.get_iccid_info(params["iccid"], iccid_table)
        logger.debug(f"iccid_info={iccid_info}")
        device_id = iccid_info["device_id"]

        # デバイス情報取得
        contract_state = 0
        device_info = ddb.get_device_info(device_id, contract_state, device_table)
        logger.debug(f"device_info={device_info}")
        if device_info is None or device_info["contract_state"] != 0:
            logger.debug("処理不要")
            return 0

        # 保守交換判定
        replace_flag = False
        if (
            device_info["device_data"]["param"]["use_type"] == 1
            and device_info["contract_state"] == 0
        ):
            logger.debug("保守交換")
            replace_flag = True

        # 旧SIM解約
        if replace_flag:
            logger.info(f"replace_flag={replace_flag}")
            contract_state = 1
            sim_info = ddb.get_device_info(device_id, contract_state, device_table)
            logger.debug(f"sim_info={sim_info}")
            coverage_url = sim_info["device_data"]["param"]["coverage_url"]
            soracom_info = ddb.get_opid_info(operator_table)
            sim_id = sim_info["device_data"]["param"]["iccid"]
            soracom_sim_terminate_api(sim_id, coverage_url, soracom_info)
            ddb.update_sim_stop(sim_info["device_id"], sim_info["imei"], device_table)
            logger.debug("旧SIM解約")

        # 初期受信日時更新
        ddb.update_init_recv(device_info["device_id"], device_info["imei"], device_table)
        return 0

    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        return -1
