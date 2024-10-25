import os
import json
from datetime import datetime

from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key, Attr
from operator import itemgetter
from decimal import Decimal
from dateutil.relativedelta import relativedelta


logger = Logger()
dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb", region_name="ap-northeast-1")
PRECISION_THRESHOLD_DISPLAYING_LOCATION_HISTORY = int(os.environ["PRECISION_THRESHOLD_DISPLAYING_LOCATION_HISTORY"])


DATE_FORMAT = "%Y/%m/%d %H:%M:%S"


###################################
# ユーザ_デバイス_グループ中間テーブル取得
#
# **kwargs :
# - gsi_name(任意) : GSI名
# - sk_prefix(任意) : 識別子
#
# 識別子:
# - ユーザID :'u-'
# - デバイスID :'d-'
# - グループID :'g-'
###################################
def get_user_device_group_table(pk, table, **kwargs):
    logger.info(f"pk:{pk}")
    if "gsi_name" in kwargs:
        # GSI PK & SK(識別子で始まる)検索
        if "sk_prefix" in kwargs:
            response = table.query(
                IndexName=kwargs["gsi_name"],
                KeyConditionExpression=Key("key2").eq(pk)
                & Key("key1").begins_with(kwargs["sk_prefix"]),
            ).get("Items", {})
        # GSI PK検索
        else:
            response = table.query(
                IndexName=kwargs["gsi_name"], KeyConditionExpression=Key("key2").eq(pk)
            ).get("Items", {})
    else:
        # PK & SK(識別子で始まる)検索
        if "sk_prefix" in kwargs:
            response = table.query(
                KeyConditionExpression=Key("key1").eq(pk)
                & Key("key2").begins_with(kwargs["sk_prefix"])
            ).get("Items", {})

        # PK検索
        else:
            response = table.query(KeyConditionExpression=Key("key1").eq(pk)).get("Items", {})

    return response


# 履歴一覧取得
def get_hist_list(hist_list_table_table, params, history_storage_period):
    sortkeyExpression = None

    unichange_hist_start = datetime.fromtimestamp(int(params["history_start_datetime"]))
    unichange_hist_end = datetime.fromtimestamp(int(params["history_end_datetime"]))
    
    # 開始日と終了日-履歴保存期間を比較
    if unichange_hist_start <= unichange_hist_end - relativedelta(years=history_storage_period):
        unichange_hist_start = unichange_hist_end - relativedelta(years=history_storage_period)
        
    change_hist_start = unichange_hist_start.timestamp()
    change_hist_end = unichange_hist_end.timestamp()
    
    if change_hist_start and change_hist_end:
        sortkeyExpression = Key("event_datetime").between(
            Decimal(int(change_hist_start) * 1000),
            Decimal(int(change_hist_end) * 1000 + 999),
        )
    elif change_hist_start:
        sortkeyExpression = Key("event_datetime").gte(
            Decimal(int(change_hist_start) * 1000)
        )
    elif change_hist_end:
        sortkeyExpression = Key("event_datetime").lte(
            Decimal(int(change_hist_end) * 1000 + 999)
        )
    reverse = params["sort"] != 0
    hist_list = []
    for device in params["device_list"]:
        last_evaluated_key = None
        if device.get("last_hist_id"):
            last_hist = hist_list_table_table.get_item(
                Key={"device_id": device["device_id"], "hist_id": device["last_hist_id"]}
            ).get("Item")
            last_evaluated_key = {
                "device_id": device["device_id"],
                "event_datetime": last_hist.get("event_datetime"),
                "hist_id": device["last_hist_id"],
            }
        device_hist_list = []
        while len(device_hist_list) < params["limit"]:
            if params["event_type_list"] == ["location_notice"]:
                query_options = {
                    "IndexName": "event_datetime_index",
                    "KeyConditionExpression": Key("device_id").eq(device["device_id"])
                    & sortkeyExpression,
                    "FilterExpression": Attr("#hist_data.#precision_state").lte(PRECISION_THRESHOLD_DISPLAYING_LOCATION_HISTORY),
                    "ExpressionAttributeNames": {
                        "#hist_data": "hist_data",
                        "#precision_state": "precision_state"
                    },
                    "ScanIndexForward": not reverse,
                    "Limit": 100,
                }
            else:
                query_options = {
                    "IndexName": "event_datetime_index",
                    "KeyConditionExpression": Key("device_id").eq(device["device_id"])
                    & sortkeyExpression,
                    "FilterExpression": Attr("hist_data.event_type").is_in(params["event_type_list"]),
                    "ScanIndexForward": not reverse,
                    "Limit": 100,
                }

            if last_evaluated_key:
                query_options["ExclusiveStartKey"] = last_evaluated_key
            logger.debug(query_options)
            res = hist_list_table_table.query(**query_options)
            logger.debug(res)
            device_hist_list.extend(res["Items"])
            last_evaluated_key = res.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break
        hist_list.extend(device_hist_list)
    logger.info(hist_list)
    return sorted(hist_list, reverse=reverse, key=lambda x: x["event_datetime"])[: params["limit"]]
