import automation
from aws_lambda_powertools import Logger

logger = Logger()


def diNameToEventDetail(terminal_state_name, device_info):
    di_list = device_info.get("device_data", {}).get("config", {}).get("terminal_settings", {}).get("di_list", [])
    for di in di_list:
        if di.get("di_on_name") == terminal_state_name:
            event_detail = "open"
            break
        elif di.get("di_off_name") == terminal_state_name:
            event_detail = "close"
            break
    return event_detail

def automationTrigger(hist_list, device_info):
    logger.debug(f"automationTrigger開始")

    for hist in hist_list:
        # 接点入力変化
        if hist["hist_data"]["event_type"] == "di_change":
            event_detail = diNameToEventDetail(hist["hist_data"]["terminal_state_name"], device_info)
            automation.automation_control(hist["device_id"], "di_change_state", hist["hist_data"]["terminal_no"], event_detail, None)
        # バッテリー残量、デバイス異常、パラメータ異常、FW更新異常
        elif hist["hist_data"]["event_type"] in ["battery_near", "device_abnormality", "parameter_abnormality", "fw_update_abnormality"]:
            automation.automation_control(hist["device_id"], hist["hist_data"]["event_type"], None, None, hist["hist_data"]["occurrence_flag"])
        # 電源オン
        elif hist["hist_data"]["event_type"]  == "power_on":
            automation.automation_control(hist["device_id"], hist["hist_data"]["event_type"], None, None, 1)
        # その他
        else:
            continue

    logger.debug("automationTrigger正常終了")
    return
