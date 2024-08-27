import re
from itertools import groupby
from operator import itemgetter

from aws_lambda_powertools import Logger

logger = Logger()


def get_device_detail(device_info, device_state, group_info_list, automation_info_list):
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
    if group_list:
        group_list = sorted(group_list, key=lambda x: x["group_name"])
    terminal_info = terminal_info_fmt(
        device_info["device_data"]["config"]["terminal_settings"],
        device_state,
    )

    formatted_automation_list = automation_info_fmt(automation_info_list)
    
    formatted_customevent_list = customevent_info_fmt(device_info["device_data"]["config"]["custom_event_list"])

    # 機器異常状態判定
    device_abnormality = 0
    if (
        device_state.get("device_abnormality", 0)
        or device_state.get("parameter_abnormality", 0)
        or device_state.get("fw_update_abnormality", 0)
        or device_state.get("device_healthy_state", 0)
    ):
        device_abnormality = 1

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
        "battery_near_status": device_state.get("battery_near_state", 0),
        "device_abnormality": device_abnormality,
        "device_healthy_period": device_info["device_data"]["config"].get(
            "device_healthy_period", 0
        ),
        "signal_status": device_state.get("signal_state", 0),
        "automation_list": formatted_automation_list,
        "custom_event_list": formatted_customevent_list,
        "di_list": terminal_info.get("di_list", ""),
        "do_list": terminal_info.get("do_list", ""),
        #'ai_list':terminal_info.get('ai_list','') #フェーズ2
    }

    return device_detail


def automation_info_fmt(automation_info_list):
    formated_automation_list = []
    for automation_item in automation_info_list:
        formated_automation_list.append(
            {
                "automation_id": automation_item["automation_id"],
                "automation_reg_datetime": automation_item.get("automation_reg_datetime", 0),
                "automation_name": automation_item["automation_name"],
                "control_device_id": automation_item["control_device_id"],
                "trigger_event_type": automation_item["trigger_event_type"],
                "trigger_terminal_no": automation_item.get("trigger_terminal_no"),
                "trigger_event_detail_state": automation_item.get("trigger_event_detail_state"),
                "trigger_event_detail_flag": automation_item.get("trigger_event_detail_flag"),
                "control_do_no": automation_item["control_do_no"],
                "control_di_state": automation_item["control_di_state"],
            }
        )
    return formated_automation_list


def terminal_info_fmt(terminal_settings, device_state):
    di_list, do_list = [], []
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

    for item in terminal_settings.get("do_list", {}):
        do_timer_list = []
        do_no = item["do_no"]
        key = f"do{do_no}_state"
        for timer_item in item.get("do_timer_list", {}):
            do_timer_list.append(
                {
                    "do_timer_id": timer_item.get("do_timer_id", ""),
                    "do_timer_reg_datetime": timer_item.get("do_timer_reg_datetime", 0),
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
                "do_state": device_state.get(key, ""),
                "do_control": item.get("do_control"),
                "do_specified_time": item.get("do_specified_time"),
                "do_di_return": item.get("do_di_return"),
                "do_timer_list": do_timer_list,
            }
        )

    logger.info(do_list)
    # アナログ入力(フェーズ2)
    return {"di_list": di_list, "do_list": do_list, "ai_list": ""}
    
def customevent_info_fmt(device_info):
    send_customevent_list = []
    di_event_list = []
    for customevent_info_list in device_info:
        for customevent_di_item in customevent_info_list.get("di_event_list", {}):
            di_event_list.append(
                {
                    "di_no": customevent_di_item.get("di_no", {}),
                    "di_state": customevent_di_item.get("di_state", {})
                }
            )
        send_customevent_list.append(
        {
            "custom_event_id": customevent_info_list["custom_event_id"],
            "custom_event_reg_datetime": customevent_info_list["custom_event_reg_datetime"],
            "event_type": customevent_info_list["event_type"],
            "custom_event_name": customevent_info_list["custom_event_name"],
            "time": customevent_info_list["time"],
            "weekday": customevent_info_list["weekday"],
            "elapsed_time": customevent_info_list["elapsed_time"],
            "di_event_list": di_event_list
        }
    )
    return send_customevent_list

    
