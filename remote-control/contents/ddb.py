import os
import decimal
import json
import time
import uuid
import db

from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
from dateutil import relativedelta

# layer
import convert

logger = Logger()

NOTIFICATION_HIST_TTL = int(os.environ["NOTIFICATION_HIST_TTL"])


def get_req_no_count_info(sim_id, table):
    response = table.get_item(Key={"simid": sim_id}).get("Item", {})
    return response


def get_remote_control_latest(device_id, do_no, table):
    response = table.query(
        IndexName="device_id_req_datetime_index",
        KeyConditionExpression=Key("device_id").eq(device_id),
        FilterExpression=Attr("do_no").eq(do_no),
        ScanIndexForward=False,  # 降順
        Limit=1,
    ).get("Items", [])
    return response


def check_control_status(device_id, do_no, delete_second, table):
    try:
        put_item = {
            "device_id": device_id,
            "do_no": do_no,
            "del_datetime": int(time.time() + delete_second),
        }
        put_item_fmt = json.loads(json.dumps(put_item), parse_float=decimal.Decimal)
        table.put_item(
            Item=put_item_fmt,
            ConditionExpression="attribute_not_exists(device_id)",
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info(e)
            return False
        else:
            raise e


def get_device_info_other_than_unavailable(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    ).get("Items", [])
    return db.insert_id_key_in_device_info_list(response)


def increment_req_no_count_num(pk, table):
    response = table.update_item(
        Key={"simid": pk},
        UpdateExpression="ADD #key :increment",
        ExpressionAttributeNames={"#key": "num"},
        ExpressionAttributeValues={":increment": decimal.Decimal(1)},
        ReturnValues="UPDATED_NEW",
    )
    count = response.get("Attributes").get("num")
    count = convert.decimal_default_proc(count)
    return count


# 通知履歴テーブル作成
def put_notification_hist(
    contract_id, notification_user_list, notification_datetime, notification_hist_table
):
    notification_hist_id = str(uuid.uuid4())
    unix_time = int(time.mktime(notification_datetime.timetuple()) * 1000)
    unix_microsecond = int(notification_datetime.microsecond / 1000)
    setting_datetime = unix_time + unix_microsecond
    expire_datetime = int((notification_datetime + relativedelta.relativedelta(years=NOTIFICATION_HIST_TTL)).timestamp())
    notice_hist_item = {
        "notification_hist_id": notification_hist_id,
        "contract_id": contract_id,
        "notification_datetime": setting_datetime,
        "expire_datetime": expire_datetime,
        "notification_user_list": notification_user_list,
    }
    item = json.loads(json.dumps(notice_hist_item), parse_float=decimal.Decimal)
    notification_hist_table.put_item(Item=item)

    return notification_hist_id
