from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Key

logger = Logger()


# OPID情報取得
def get_opid_info(operator_table):
    opid_list = operator_table.query(KeyConditionExpression=Key("service").eq("monosc")).get(
        "Items"
    )
    return opid_list[0] if opid_list else None
