from aws_lambda_powertools import Logger

logger = Logger()


# 未登録デバイス取得(iemi)
def get_pre_reg_device_info_by_imei(imei, table):
    pre_register_device_info = table.get_item(Key={"imei": imei}).get("Item", {})
    return pre_register_device_info
