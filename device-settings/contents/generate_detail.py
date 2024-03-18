import re

from aws_lambda_powertools import Logger

logger = Logger()


def get_device_detail(device_info, device_state, group_info_list):
    last_receiving_time = ""
    group_list = []
    # 最終受信日時取得
    if device_state:
        pattern = re.compile(r".*update_datetime$")
        matching_keys = [key for key in device_state.keys() if pattern.match(key)]
        matching_values = {key: device_state[key] for key in matching_keys}
        last_receiving_time = max(matching_values.values())
    for item in group_info_list:
        group_list.append(
            {
                "group_id": item["group_id"],
                "group_name": item["group_data"]["config"]["group_name"],
            }
        )

    # レスポンス生成
    device_detail = {
        "message": "",
        "device_id": device_info["device_id"],
        "device_name": device_info["device_data"]["config"]["device_name"],
        "device_code": device_info["device_data"]["param"]["device_code"],
        "device_iccid": device_info["device_data"]["param"]["iccid"],
        "device_imei": device_info["imei"],
        "device_type": device_info["device_type"],
        "group_list": group_list,
        "last_receiving_time": last_receiving_time,
        "signal_status": device_state.get("signal_state", 0),
        "battery_near_status": device_state.get("battery_near_state", 0),
        "device_healthy_period": device_info["device_data"]["config"].get(
            "device_healthy_period", 0
        ),
    }

    return device_detail
