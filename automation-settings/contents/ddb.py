from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key

logger = Logger()


# 連動制御設定取得(デバイスに紐づく)
def get_automation_info_device(device_id, table):
    response = table.query(
        IndexName="control_device_id_index",
        KeyConditionExpression=Key("control_device_id").eq(device_id),
    ).get("Items", [])
    return response
