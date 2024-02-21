import json
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger

dynamodb = boto3.resource("dynamodb")
logger = Logger()


def decimal_to_num(obj):
    if isinstance(obj, decimal.Decimal):
        return int(obj) if float(obj).is_integer() else float(obj)


# 全契約ID取得
def get_contract_id_list(contract_table):
    contract_id_list = []
    response = contract_table.scan()
    contract_info_list = response.get("Items")

    while "LastEvaluatedKey" in response:
        response = contract_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        contract_info_list.extend(response['Items'])

    for contract_info in contract_info_list:
        device_list = contract_info.get("contract_data", {}).get("device_list", [])
        if len(device_list) > 0:
            contract_id_list.append(contract_info.get("contract_id"))
    logger.debug(f"contract_id_list={contract_id_list}")
    return contract_id_list
