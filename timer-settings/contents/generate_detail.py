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
        "do_list": terminal_info.get("do_list", ""),
    }

    return device_detail


def terminal_info_fmt(terminal_settings, device_state):
    do_list = []

    for item in terminal_settings.get("do_list", {}):
        do_timer_list = []
        do_no = item["do_no"]
        for timer_item in item.get("do_timer_list", {}):
            do_timer_list.append(
                {
                    "do_timer_id": timer_item.get("do_timer_id", ""),
                    "do_timer_name": timer_item.get("do_timer_name", ""),
                    "do_onoff_control": timer_item.get("do_onoff_control", ""),
                    "do_time": timer_item.get("do_time", ""),
                    "do_weekday": timer_item.get("do_weekday", ""),
                }
            )
        do_list.append(
            {
                "do_no": do_no,
                "do_name": item.get("do_name", ""),
                "do_timer_list": do_timer_list,
            }
        )

    logger.info(do_list)
    # アナログ入力(フェーズ2)

    return {"do_list": do_list}
