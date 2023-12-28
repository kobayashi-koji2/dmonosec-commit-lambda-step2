import json
import os
import logging
import traceback
from decimal import Decimal
import time

import boto3
from botocore.exceptions import ClientError

import ssm
import convert
import ddb
import validate

parameter = None
logger = logging.getLogger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]


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
            contract_table = dynamodb.Table(parameter.get("CONTRACT_TABLE"))
            device_relation_table = dynamodb.Table(
                parameter.get("DEVICE_RELATION_TABLE")
            )
            remote_controls_table = dynamodb.Table(
                parameter.get("REMOTE_CONTROL_TABLE")
            )
            cnt_hist_table = dynamodb.Table(parameter.get("CNT_HIST_TABLE"))
            hist_list_table = dynamodb.Table(parameter.get("HIST_LIST_TABLE"))
            device_table = dynamodb.Table(parameter.get("DEVICE_TABLE"))
            group_table = dynamodb.Table(parameter.get("GROUP_TABLE"))
        except KeyError as e:
            parameter = None
            body = {"code": "9999", "message": e}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(body, ensure_ascii=False),
            }
        # パラメータチェック
        validate_result = validate.validate(
            event,
            contract_table,
            user_table,
            device_relation_table,
            remote_controls_table,
        )
        if validate_result["code"] != "0000":
            raise Exception(json.dumps(validate_result))

        remote_control = validate_result["remote_control"]

        link_di_no = remote_control["link_di_no"]

        req_datetime = remote_control["req_datetime"]
        limit_datetime = req_datetime + 10000  # 10秒
        if time.time() <= limit_datetime / 1000:
            # タイムアウト時間まで待機
            time.sleep(limit_datetime / 1000 - time.time())

        remote_control = ddb.get_remote_control_info(
            remote_control["device_req_no"], remote_controls_table
        )
        if remote_control.get("control_result") is None:
            # タイムアウト
            # TODO メール通知

            # 履歴レコード作成
            ddb.put_hist_list(
                remote_control,
                None,
                "timeout_response",
                hist_list_table,
                device_table,
                group_table,
                device_relation_table,
            )
            return

        if link_di_no is not None:
            # 接点入力紐づけ設定あり
            recv_datetime = remote_control["recv_datetime"]
            limit_datetime = recv_datetime + 20000  # 20秒
            if time.time() <= limit_datetime / 1000:
                # タイムアウト時間まで待機
                time.sleep(limit_datetime / 1000 - time.time())

        cnt_hist_list = ddb.get_cnt_hist(
            remote_control["iccid"], recv_datetime, limit_datetime, cnt_hist_table
        )
        if not [
            cnt_hist
            for cnt_hist in cnt_hist_list
            if cnt_hist.get("di_trigger") == link_di_no
        ]:
            # TODO メール通知

            # 履歴レコード作成
            ddb.put_hist_list(
                remote_control,
                None,
                "timeout_status",
                hist_list_table,
                device_table,
                group_table,
                device_relation_table,
            )

    except Exception as e:
        print(e)
        print(traceback.format_exc())
        res_body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        raise Exception(json.dumps(res_body))
