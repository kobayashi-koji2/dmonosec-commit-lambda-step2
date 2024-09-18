from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger

logger = Logger()


# 未登録デバイス取得(iemi)
def get_pre_reg_device_info_by_imei(imei, table):
    pre_register_device_info = table.get_item(Key={"identification_id": imei}).get("Item", {})
    return pre_register_device_info

# デバイスお知らせ管理情報取得
def get_device_announcement_list(device_announcement_table, imei):
    device_announcement_list = device_announcement_table.query(
        IndexName="imei_announcement_type_index",
        KeyConditionExpression=Key("imei").eq(imei) & Key("device_announcement_type").eq("regist_balance_days")
    ).get("Items",[])

    return device_announcement_list[0] if device_announcement_list else None
