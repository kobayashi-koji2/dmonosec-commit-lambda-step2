from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key

logger = Logger()


# 連動制御設定取得
def get_automation_info(automation_id, table):
    response = table.get_item(
        Key={"automation_id": automation_id},
    ).get("Items")
    return response
