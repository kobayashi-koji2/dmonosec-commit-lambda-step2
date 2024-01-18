import decimal

from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key, Attr

# layer
import convert

logger = Logger()


def get_device_info_available(table):
    response = table.scan(
        FilterExpression=Attr("contract_state").eq(1),
    ).get("Items", [])
    return response


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
