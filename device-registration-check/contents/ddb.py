from boto3.dynamodb.conditions import Attr


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
    return scan_response["Items"]
