from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key

logger = Logger()


# デバイス情報取得
def get_device_info(device_id, table):
    device_list = table.query(KeyConditionExpression=Key("device_id").eq(device_id)).get(
        "Items", []
    )
    return device_list[0] if device_list else None
