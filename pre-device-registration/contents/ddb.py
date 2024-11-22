from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger

logger = Logger()


# 未登録デバイス取得
def get_pre_reg_device_info_by_imei(imei, contract_id, table):
    pre_register_device_info = table.get_item(Key={"identification_id": imei}).get("Item", {})
    if pre_register_device_info and pre_register_device_info.get("contract_id") != contract_id:
        pre_register_device_info = {}
    return pre_register_device_info
