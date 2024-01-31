import os
import base64
import boto3
import ddb
import json
import ssm
import time
import traceback
import validate
from datetime import datetime
from command_parser import commandParser
from aws_lambda_powertools import Logger

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
INITIAL_LAMBDA_NAME = os.environ["INITIAL_LAMBDA_NAME"]

logger = Logger()


def lambda_handler(event, context):
    logger.debug(f"lambda_handler開始 event={event}")

    try:
        # DynamoDB操作オブジェクト生成
        try:
            iccid_table = dynamodb.Table(ssm.table_names["ICCID_TABLE"])
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            hist_table = dynamodb.Table(ssm.table_names["CNT_HIST_TABLE"])
            hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
            state_table = dynamodb.Table(ssm.table_names["STATE_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            notification_hist_table = dynamodb.Table(ssm.table_names["NOTIFICATION_HIST_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            user_table = dynamodb.Table(ssm.table_names["USER_TABLE"])
            account_table = dynamodb.Table(ssm.table_names["ACCOUNT_TABLE"])
            remote_control_table = dynamodb.Table(ssm.table_names["REMOTE_CONTROL_TABLE"])
        except KeyError as e:
            logger.error("KeyError")
            return bytes([3])

        # サーバー受信日時を取得
        now = datetime.now()
        szRecvDatetime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)

        # 受信データ取り出し
        Payload = base64.standard_b64decode(event["payload"])
        szSimid = context.client_context.custom["simId"]
        logger.info(f"Payload={Payload.hex()}, szSimid={szSimid}, szRecvDatetime={szRecvDatetime}")

        # 入力データチェック
        vali_result = validate.validate(Payload, szSimid, szRecvDatetime, hist_table)
        if vali_result != 0:
            if vali_result == 5:
                return bytes([1])
            else:
                return bytes([2])

        # ICCID情報取得
        device_id = None
        device_info = None
        stray_flag = True
        iccid_info = ddb.get_iccid_info(szSimid, iccid_table)
        if iccid_info is None or len(iccid_info) == 0:
            logger.debug(f"未登録デバイス szSimid={szSimid}")
        else:
            stray_flag = False
            device_id = iccid_info["device_id"]
            device_info = ddb.get_device_info(device_id, device_table)
            if device_info is None or len(device_info) == 0:
                return bytes([1])

        # データ解析・登録
        try:
            res = commandParser(
                szSimid,
                szRecvDatetime,
                Payload,
                device_info,
                stray_flag,
                hist_table,
                hist_list_table,
                state_table,
                group_table,
                notification_hist_table,
                device_relation_table,
                user_table,
                account_table,
                remote_control_table,
            )
        except Exception as e:
            logger.debug(f"commandParserエラー e={e}")
            return bytes([3])

        ##################
        # 初期受信処理
        ##################
        if (not stray_flag) and (device_info.get("contract_state") == 0):
            # パラメータ設定
            input_event = {"iccid": szSimid}
            Payload = json.dumps(input_event)

            # 呼び出し
            boto3.client("lambda").invoke(
                FunctionName=INITIAL_LAMBDA_NAME, InvocationType="Event", Payload=Payload
            )
            logger.debug("initialreceive呼び出し")

        logger.debug("lambda_handler正常終了")
        return res

    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        return bytes([3])
