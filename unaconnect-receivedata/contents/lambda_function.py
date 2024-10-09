import os
import base64
import boto3
import ddb
import json
import ssm
import time
import traceback
import validate
import uuid
from datetime import datetime
from dateutil import relativedelta
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from event_judge import eventJudge

patch_all()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))
sqs = boto3.resource("sqs", endpoint_url=os.environ.get("endpoint_url"))

logger = Logger()

res_headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}

DEVICE_HEALTHY_CHECK_SQS_QUEUE_NAME = os.environ["DEVICE_HEALTHY_CHECK_SQS_QUEUE_NAME"]
HIST_LIST_TTL = int(os.environ["HIST_LIST_TTL"])
CNT_HIST_TTL = int(os.environ["CNT_HIST_TTL"])

def lambda_handler(event, context):
    logger.debug(f"lambda_handler開始 event={event}")
    rec_body = json.loads(event.get("body", {}))

    try:
        # DynamoDB操作オブジェクト生成
        try:
            device_table = dynamodb.Table(ssm.table_names["DEVICE_TABLE"])
            hist_table = dynamodb.Table(ssm.table_names["CNT_HIST_TABLE"])
            hist_list_table = dynamodb.Table(ssm.table_names["HIST_LIST_TABLE"])
            state_table = dynamodb.Table(ssm.table_names["STATE_TABLE"])
            group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
            device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])
            sigfox_id_table = dynamodb.Table(ssm.table_names["SIGFOX_ID_TABLE"])
        except KeyError as e:
            logger.error("KeyError")
            logger.error(traceback.format_exc())
            body = {"message": "予期しないエラーが発生しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            } 

        #受信日時を取得
        recv_datetime = int(time.time() * 1000)
        expire_datetime = int(
            (
                datetime.fromtimestamp(recv_datetime / 1000)
                + relativedelta.relativedelta(years=CNT_HIST_TTL)
            ).timestamp()
        )

        # 入力データチェック
        vali_result = validate.validate(rec_body)
        if vali_result.get("message"):
            logger.info("Error in validation check of input information.")
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(vali_result, ensure_ascii=False),
            }

        hist_info_id = str(uuid.uuid4())
        # 履歴情報テーブルへデータ格納
        db_item = {
            "cnt_hist_id": hist_info_id,
            "sigfox_id": rec_body.get("deviceId"),
            "event_datetime": rec_body.get("timestamp") * 1000,
            "recv_datetime": recv_datetime,
            "datetime": rec_body.get("dateTime"),
            "device_name": rec_body.get("deviceName"),
            "expire_datetime":expire_datetime,
            "unaconnect_group_id": rec_body.get("groupID"),
            "source_label":rec_body.get("sourceLabel"),
            "signal_score":rec_body.get("signalScore"),
            "num_bs":rec_body.get("numBS"),
            "sigfox_rc":rec_body.get("rc"),
            "rssi":rec_body.get("rssi"),
            "duplicates":rec_body.get("duplicates"),
            "data_type":rec_body.get("dataType"),
            "battery_voltage":rec_body.get("batteryVoltage"),
            "data":rec_body.get("data"),
            "device_type":rec_body.get("deviceType"),
        }
        ddb.put_db_item(db_item,hist_table)

        device_id = ddb.get_device_id_by_sigfox_id_info(rec_body.get("deviceId"),sigfox_id_table)
        group_list =  ddb.get_device_group_list(device_id, device_relation_table, group_table)
        expire_datetime = int(
            (
                datetime.fromtimestamp(recv_datetime / 1000)
                + relativedelta.relativedelta(years=HIST_LIST_TTL)
            ).timestamp()
        )
        #履歴一覧テーブル更新
        if rec_body.get("dataType") == "GEOLOC":
            db_item = {
                "device_id": device_id,
                "hist_id": str(uuid.uuid4()),
                "event_datetime": rec_body.get("timestamp") * 1000,
                "recv_datetime": recv_datetime,
                "expire_datetime": expire_datetime,
                "hist_data": {
                    "device_name":rec_body.get("deviceName"),
                    "sigfox_id":rec_body.get("deviceId"),
                    "event_type":"location_notice",
                    "cnt_hist_id":hist_info_id,
                    "group_list":group_list,
                    "latitude_state":rec_body.get("data").get("lat"),
                    "longitude_state":rec_body.get("data").get("lng"),
                    "precision_state":rec_body.get("data").get("radius")
                }
            }
            ddb.put_db_item(db_item,hist_list_table)

        #現状態の取得
        device_current_state = ddb.get_device_state(device_id, state_table)
        logger.debug(f"device_current_state={device_current_state}")
        #現状態と受信した状態を比較、更新。
        current_state_info = eventJudge(rec_body,device_current_state,device_id)
        logger.debug(f"current_state_info={current_state_info}")
        ddb.put_db_item(current_state_info,state_table)

        # デバイスヘルシー判定
        queue = sqs.get_queue_by_name(QueueName=DEVICE_HEALTHY_CHECK_SQS_QUEUE_NAME)
        if current_state_info.get(
            "device_healthy_state"
        ) == 1 and current_state_info.get(
            "latitude_last_update_datetime"
        ) != device_current_state.get(
            "latitude_last_update_datetime"
        ):
            body = {
                "event_trigger": "lambda-unaconnect-receivedata",
                "event_type": "device_unhealthy",
                "event_datetime": rec_body.get("timestamp",""),
                "device_id": device_id,
            }
            queue.send_message(DelaySeconds=0, MessageBody=(json.dumps(body)))
        
        #メッセージ応答
        res_body = {"message": ""}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        res_body = {"message": ""}
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        } 
