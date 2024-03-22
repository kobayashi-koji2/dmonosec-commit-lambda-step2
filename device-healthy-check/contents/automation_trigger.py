import automation
from aws_lambda_powertools import Logger

logger = Logger()


def automationTrigger(hist_list):
    logger.debug(f"automationTrigger開始")

    for hist in hist_list:
        # デバイスヘルシー
        if hist["hist_data"]["event_type"] == "device_unhealthy":
            automation.automation_control(hist["device_id"], "device_unhealthy", None, None, hist["hist_data"]["occurrence_flag"])
        # 接点入力未変化
        elif hist["hist_data"]["event_type"]  == "di_unhealthy":
            automation.automation_control(hist["device_id"], "di_unhealthy", hist["hist_data"]["terminal_no"], None, hist["hist_data"]["occurrence_flag"])
        # その他
        else:
            continue

    logger.debug("automationTrigger正常終了")
    return
