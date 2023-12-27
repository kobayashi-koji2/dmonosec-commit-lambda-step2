import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key, Attr
from operator import itemgetter
from decimal import Decimal


dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb", region_name="ap-northeast-1")


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
    print(f"pk:{pk}")
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
            response = table.query(KeyConditionExpression=Key("key1").eq(pk)).get(
                "Items", {}
            )

    return response


# 履歴一覧取得
def get_hist_list(hist_list_table_table, params):
    sortkeyExpression = None
    if params["history_start_datetime"] and params["history_end_datetime"]:
        sortkeyExpression = Key("event_datetime").between(
            Decimal(int(params["history_start_datetime"]) * 1000),
            Decimal(int(params["history_end_datetime"]) * 1000 + 999),
        )
    elif params["history_start_datetime"]:
        sortkeyExpression = Key("event_datetime").gte(
            Decimal(int(params["history_start_datetime"]) * 1000)
        )
    elif params["history_end_datetime"]:
        sortkeyExpression = Key("event_datetime").lte(
            Decimal(int(params["history_end_datetime"]) * 1000 + 999)
        )
    hist_list = []
    for device_id in params["device_list"]:
        res = hist_list_table_table.query(
            IndexName="event_datetime_index",
            KeyConditionExpression=Key("device_id").eq(device_id) & sortkeyExpression,
            FilterExpression=Attr("hist_data.event_type").is_in(
                params["event_type_list"]
            ),
        )
        hist_list.extend(res["Items"])
    print(hist_list)
    return sorted(hist_list, key=lambda x: x["event_datetime"])
