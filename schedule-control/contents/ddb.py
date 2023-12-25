import os
import decimal

from boto3.dynamodb.conditions import Key, Attr

# layer
import convert

AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
ENDPOINT_URL = os.environ["endpoint_url"]

def get_device_info_available(table):
    response = table.scan(
        FilterExpression=Attr("contract_state").eq(1),
    ).get("Items", [])
    return response


def get_req_no_count_info(sim_id, table):
    response = table.get_item(
        Key={"simid": sim_id}
    ).get("Item", {})
    return response


def get_remote_control_latest(pk, filter, table):
    response = table.query(
        KeyConditionExpression=Key("device_req_no").eq(pk),
        FilterExpression=Attr("do_no").eq(filter),
        ScanIndexForward = False, # 降順
        Limit = 1
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
