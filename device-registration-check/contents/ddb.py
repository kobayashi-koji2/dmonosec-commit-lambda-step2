from boto3.dynamodb.conditions import Key,Attr
import db


def get_pre_register_device_list(
    pre_register_device_table, low_dev_reg_datetime=None, high_dev_reg_datetime=None
):
    params = {}
    if low_dev_reg_datetime and high_dev_reg_datetime:
        params["FilterExpression"] = Attr("dev_reg_datetime").between(
            low_dev_reg_datetime, high_dev_reg_datetime
        )
    elif low_dev_reg_datetime:
        params["FilterExpression"] = Attr("dev_reg_datetime").gte(low_dev_reg_datetime)
    elif high_dev_reg_datetime:
        params["FilterExpression"] = Attr("dev_reg_datetime").lte(high_dev_reg_datetime)

    scan_response = pre_register_device_table.scan(**params)
    return db.insert_id_key_in_device_info_list(scan_response["Items"])

# デバイスお知らせ管理情報取得
def get_device_announcement_list(device_announcement_table, identification_id):
    device_announcement_list = device_announcement_table.query(
        IndexName="identification_id_announcement_type_index",
        KeyConditionExpression=Key("identification_id").eq(identification_id) & Key("device_announcement_type").eq("regist_balance_days")
    ).get("Items",[])

    return device_announcement_list[0] if device_announcement_list else None
