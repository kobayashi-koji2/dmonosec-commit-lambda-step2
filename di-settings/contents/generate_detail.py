from aws_lambda_powertools import Logger

logger = Logger()


def get_device_detail(device_info, device_state, group_info_list):
    group_list = []
    for item in group_info_list:
        group_list.append(
            {
                "group_id": item["group_id"],
                "group_name": item["group_data"]["config"]["group_name"],
            }
        )
    terminal_info = terminal_info_fmt(
        device_info["device_data"]["config"]["terminal_settings"], device_state
    )

    # レスポンス生成
    device_detail = {
        "message": "",
        "di_list": terminal_info.get("di_list", ""),
    }

    return device_detail


def terminal_info_fmt(terminal_settings, device_state):
    di_list = []
    for item in terminal_settings.get("di_list", {}):
        di_no = item["di_no"]
        di_state_key = f"di{di_no}_state"
        di_healthy_state_key = f"di{di_no}_healthy_state"
        di_last_change_datetime_key = f"di{di_no}_last_change_datetime"
        di_list.append(
            {
                "di_no": di_no,
                "di_name": item.get("di_name", ""),
                "di_state": device_state.get(di_state_key, ""),
                "di_on_name": item.get("di_on_name", ""),
                "di_on_icon": item.get("di_on_icon", ""),
                "di_off_name": item.get("di_off_name", ""),
                "di_off_icon": item.get("di_off_icon", ""),
                "di_healthy_type": item.get("di_healthy_type", ""),
                "di_healthy_period": item.get("di_healthy_period", 0),
                "di_healthy_state": device_state.get(di_healthy_state_key, 0),
                "di_last_change_datetime": device_state.get(di_last_change_datetime_key, 0),
            }
        )

    logger.info(di_list)
    # アナログ入力(フェーズ2)

    return {"di_list": di_list}
