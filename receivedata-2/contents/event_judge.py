import os
import ddb
import uuid
from datetime import datetime
from dateutil import relativedelta
from aws_lambda_powertools import Logger

logger = Logger()

RSSI_HIGH_MIN = int(os.environ["RSSI_HIGH_MIN"])
RSSI_HIGH_MAX = int(os.environ["RSSI_HIGH_MAX"])
RSSI_MID_MIN = int(os.environ["RSSI_MID_MIN"])
RSSI_MID_MAX = int(os.environ["RSSI_MID_MAX"])
RSSI_LOW_MIN = int(os.environ["RSSI_LOW_MIN"])
RSSI_LOW_MAX = int(os.environ["RSSI_LOW_MAX"])
SINR_HIGH_MIN = int(os.environ["SINR_HIGH_MIN"])
SINR_HIGH_MAX = int(os.environ["SINR_HIGH_MAX"])
SINR_MID_MIN = int(os.environ["SINR_MID_MIN"])
SINR_MID_MAX = int(os.environ["SINR_MID_MAX"])
SINR_LOW_MIN = int(os.environ["SINR_LOW_MIN"])
SINR_LOW_MAX = int(os.environ["SINR_LOW_MAX"])
SIGNAL_HIGH = int(os.environ["SIGNAL_HIGH"])
SIGNAL_MID = int(os.environ["SIGNAL_MID"])
SIGNAL_LOW = int(os.environ["SIGNAL_LOW"])
NO_SIGNAL = int(os.environ["NO_SIGNAL"])
HIST_LIST_TTL = int(os.environ["HIST_LIST_TTL"])


def createHistListData(recv_data, device_info, event_info, device_relation_table, group_table):
    # グループ情報取得
    group_list = []
    group_list = ddb.get_device_group_list(
        device_info.get("device_id"), device_relation_table, group_table
    )
    logger.debug(f"group_list={group_list}")

    # 共通部
    expire_datetime = int(
        (
            datetime.fromtimestamp(recv_data.get("recv_datetime") / 1000)
            + relativedelta.relativedelta(years=HIST_LIST_TTL)
        ).timestamp()
    )
    hist_list_data = {
        "device_id": device_info.get("device_id"),
        "hist_id": str(uuid.uuid4()),
        "event_datetime": event_info.get("event_datetime"),
        "recv_datetime": recv_data.get("recv_datetime"),
        "expire_datetime": expire_datetime,
        "hist_data": {
            "device_name": device_info.get("device_data", {}).get("config", {}).get("device_name"),
            "group_list": group_list,
            "imei": device_info.get("imei"),
            "event_type": event_info.get("event_type"),
        },
    }

    if recv_data.get("message_type") in ["0001", "0011", "0012"]:
        hist_list_data["hist_data"]["cnt_hist_id"] = recv_data.get("cnt_hist_id")

    # 接点入力部
    if event_info.get("event_type") == "di_change":
        terminal_no = event_info.get("terminal_no")
        for di_list in (
            device_info.get("device_data", {})
            .get("config", {})
            .get("terminal_settings", {})
            .get("di_list", [])
        ):
            if int(di_list.get("di_no")) == int(terminal_no):
                terminal_name = di_list.get("di_name", f"接点入力{terminal_no}")
                if event_info.get("di_state") == 0:
                    terminal_state_name = di_list.get("di_on_name", "クローズ")
                else:
                    terminal_state_name = di_list.get("di_off_name", "オープン")
                break
        hist_list_data["hist_data"]["terminal_no"] = terminal_no
        hist_list_data["hist_data"]["terminal_name"] = terminal_name
        hist_list_data["hist_data"]["terminal_state_name"] = terminal_state_name

    # 接点出力部
    elif event_info.get("event_type") == "do_change":
        terminal_no = event_info.get("terminal_no")
        for do_list in (
            device_info.get("device_data", {})
            .get("config", {})
            .get("terminal_settings", {})
            .get("do_list", [])
        ):
            if int(do_list.get("do_no")) == int(terminal_no):
                terminal_name = do_list.get("do_name", f"接点出力{terminal_no}")
                if event_info.get("do_state") == 0:
                    terminal_state_name = "クローズ"
                else:
                    terminal_state_name = "オープン"
                break
        hist_list_data["hist_data"]["terminal_no"] = terminal_no
        hist_list_data["hist_data"]["terminal_name"] = terminal_name
        hist_list_data["hist_data"]["terminal_state_name"] = terminal_state_name

    # デバイス状態
    elif event_info.get("event_type") in [
        "battery_near",
        "device_abnormality",
        "parameter_abnormality",
        "fw_update_abnormality",
    ]:
        hist_list_data["hist_data"]["occurrence_flag"] = event_info.get("occurrence_flag")

    # 接点出力制御応答
    elif event_info.get("event_type") in [
        "manual_control",
        "on_timer_control",
        "off_timer_control",
        "timer_control",
        "automation_control",
        "on_automation_control",
        "off_automation_control",
    ]:
        if "link_di_no" in event_info:
            link_di_no = event_info["link_di_no"]
            hist_list_data["hist_data"]["link_terminal_no"] = event_info.get("link_di_no")
            for di_list in (
                device_info.get("device_data", {})
                .get("config", {})
                .get("terminal_settings", {})
                .get("di_list", [])
            ):
                if int(di_list.get("di_no")) == int(event_info.get("link_di_no")):
                    di_terminal_name = di_list.get("di_name", f"接点入力{link_di_no}")
                    if event_info.get("di_state") == 0:
                        terminal_state_name = di_list.get("di_on_name", "クローズ")
                    else:
                        terminal_state_name = di_list.get("di_off_name", "オープン")
                    break
            hist_list_data["hist_data"]["link_terminal_name"] = di_terminal_name
            hist_list_data["hist_data"]["link_terminal_state_name"] = terminal_state_name
            hist_list_data["hist_data"]["control_trigger"] = event_info.get("control_trigger")
            hist_list_data["hist_data"]["terminal_no"] = int(event_info.get("do_no"))
            for do_list in (
                device_info.get("device_data", {})
                .get("config", {})
                .get("terminal_settings", {})
                .get("do_list", [])
            ):
                if int(do_list.get("do_no")) == int(event_info.get("do_no")):
                    do_no = do_list.get("do_no")
                    do_terminal_name = do_list.get("do_name", f"接点出力{do_no}")
                    break
            hist_list_data["hist_data"]["terminal_name"] = do_terminal_name
            if event_info.get("event_type") == "manual_control":
                hist_list_data["hist_data"]["control_exec_user_name"] = event_info.get(
                    "control_exec_user_name"
                )
                hist_list_data["hist_data"]["control_exec_user_email_address"] = event_info.get(
                    "control_exec_user_email_address"
                )
            hist_list_data["hist_data"]["control_result"] = event_info.get("control_result")
            hist_list_data["hist_data"]["device_req_no"] = event_info.get("device_req_no")
            if event_info.get("event_type") in [
                "on_timer_control",
                "off_timer_control",
                "timer_control",
            ]:
                hist_list_data["hist_data"]["timer_time"] = event_info.get("timer_time")
            if event_info.get("event_type") in [
                "automation_control",
                "on_automation_control",
                "off_automation_control",
            ]:
                hist_list_data["hist_data"]["automation_trigger_device_name"] = event_info.get(
                    "automation_trigger_device_name"
                )
                hist_list_data["hist_data"]["automation_trigger_imei"] = event_info.get(
                    "automation_trigger_imei"
                )
                hist_list_data["hist_data"]["automation_trigger_event_type"] = event_info.get(
                    "automation_trigger_event_type"
                )
                hist_list_data["hist_data"]["automation_trigger_terminal_no"] = event_info.get(
                    "automation_trigger_terminal_no"
                )
                hist_list_data["hist_data"]["automation_trigger_event_detail_state"] = (
                    event_info.get("automation_trigger_event_detail_state")
                )
                hist_list_data["hist_data"]["automation_trigger_event_detail_flag"] = (
                    event_info.get("automation_trigger_event_detail_flag")
                )
        else:
            hist_list_data["hist_data"]["control_trigger"] = event_info.get("control_trigger")
            for do_list in (
                device_info.get("device_data", {})
                .get("config", {})
                .get("terminal_settings", {})
                .get("do_list", [])
            ):
                if int(do_list.get("do_no")) == int(event_info.get("do_no")):
                    do_no = do_list.get("do_no")
                    terminal_name = do_list.get("do_name", f"接点出力{do_no}")
                    break
            hist_list_data["hist_data"]["terminal_name"] = terminal_name
            hist_list_data["hist_data"]["terminal_no"] = int(event_info.get("do_no"))
            if event_info.get("event_type") == "manual_control":
                hist_list_data["hist_data"]["control_exec_user_name"] = event_info.get(
                    "control_exec_user_name"
                )
                hist_list_data["hist_data"]["control_exec_user_email_address"] = event_info.get(
                    "control_exec_user_email_address"
                )
            hist_list_data["hist_data"]["control_result"] = event_info.get("control_result")
            hist_list_data["hist_data"]["device_req_no"] = event_info.get("device_req_no")
            if event_info.get("event_type") in [
                "on_timer_control",
                "off_timer_control",
                "timer_control",
            ]:
                hist_list_data["hist_data"]["timer_time"] = event_info.get("timer_time")
            if event_info.get("event_type") in [
                "automation_control",
                "on_automation_control",
                "off_automation_control",
            ]:
                hist_list_data["hist_data"]["automation_trigger_device_name"] = event_info.get(
                    "automation_trigger_device_name"
                )
                hist_list_data["hist_data"]["automation_trigger_imei"] = event_info.get(
                    "automation_trigger_imei"
                )
                hist_list_data["hist_data"]["automation_trigger_event_type"] = event_info.get(
                    "automation_trigger_event_type"
                )
                hist_list_data["hist_data"]["automation_trigger_terminal_no"] = event_info.get(
                    "automation_trigger_terminal_no"
                )
                hist_list_data["hist_data"]["automation_trigger_event_detail_state"] = (
                    event_info.get("automation_trigger_event_detail_state")
                )
                hist_list_data["hist_data"]["automation_trigger_event_detail_flag"] = (
                    event_info.get("automation_trigger_event_detail_flag")
                )
    return hist_list_data


def initCurrentStateInfo(recv_data, device_current_state, device_info, init_state_flg):
    if init_state_flg == 1:
        di_list = list(reversed(list(recv_data.get("di_state", []))))
        do_list = list(reversed(list(recv_data.get("do_state", []))))

        current_state_info = {
            "device_id": device_info.get("device_id"),
            "signal_last_update_datetime": recv_data.get("recv_datetime"),
            "battery_near_last_update_datetime": recv_data.get("recv_datetime"),
            "device_abnormality_last_update_datetime": recv_data.get("recv_datetime"),
            "parameter_abnormality_last_update_datetime": recv_data.get("recv_datetime"),
            "fw_update_abnormality_last_update_datetime": recv_data.get("recv_datetime"),
            "di1_last_update_datetime": recv_data.get("recv_datetime"),
            "signal_state": 0,
            "battery_near_state": 0,
            "device_abnormality": 0,
            "parameter_abnormality": 0,
            "fw_update_abnormality": 0,
            "di1_state": int(di_list[0]) if di_list else None,
        }

        if recv_data.get("device_type") in ["PJ2", "PJ3"]:
            current_state_info["di2_state"] = int(di_list[1]) if di_list else None
            current_state_info["di3_state"] = int(di_list[2]) if di_list else None
            current_state_info["di4_state"] = int(di_list[3]) if di_list else None
            current_state_info["di5_state"] = int(di_list[4]) if di_list else None
            current_state_info["di6_state"] = int(di_list[5]) if di_list else None
            current_state_info["di7_state"] = int(di_list[6]) if di_list else None
            current_state_info["di8_state"] = int(di_list[7]) if di_list else None
            current_state_info["do1_state"] = int(do_list[0]) if do_list else None
            current_state_info["do2_state"] = int(do_list[1]) if do_list else None
            current_state_info["di2_last_update_datetime"] = recv_data.get("recv_datetime")
            current_state_info["di3_last_update_datetime"] = recv_data.get("recv_datetime")
            current_state_info["di4_last_update_datetime"] = recv_data.get("recv_datetime")
            current_state_info["di5_last_update_datetime"] = recv_data.get("recv_datetime")
            current_state_info["di6_last_update_datetime"] = recv_data.get("recv_datetime")
            current_state_info["di7_last_update_datetime"] = recv_data.get("recv_datetime")
            current_state_info["di8_last_update_datetime"] = recv_data.get("recv_datetime")
            current_state_info["do1_last_update_datetime"] = recv_data.get("recv_datetime")
            current_state_info["do2_last_update_datetime"] = recv_data.get("recv_datetime")

            """
            if recv_data.get("device_type") == "PJ3":
                current_state_info["ai1_state"] = recv_data.get("analogv1")
                current_state_info["ai2_state"] = recv_data.get("analogv2")
                current_state_info["ai1_last_update_datetime"] = recv_data.get("recv_datetime")
                current_state_info["ai2_last_update_datetime"] = recv_data.get("recv_datetime")
                current_state_info["ai1_threshold_last_update_datetime"] = recv_data.get("recv_datetime")
                current_state_info["ai2_threshold_last_update_datetime"] = recv_data.get("recv_datetime")
            """

            device_state_custom_timer_event_list = []
            for custom_event in device_info.get("device_data").get("config").get("custom_event_list", []):
                if custom_event.get("event_type") == 1:
                    custom_timer_event = {}
                    custom_timer_event["custom_event_id"] = custom_event.get("custom_event_id")
                    custom_timer_event["elapsed_time"] = custom_event.get("elapsed_time")
                    device_state_di_event_list = []
                    for di_event in custom_event.get("di_event_list", []):
                        device_state_di_event = {}
                        device_state_di_event["di_no"] = di_event["di_no"]
                        device_state_di_event["di_state"] = di_event["di_state"]
                        device_state_di_event["event_datetime"] = 0
                        device_state_di_event["event_judge_datetime"] = 0
                        device_state_di_event["delay_flag"] = 0
                        device_state_di_event_list.append(device_state_di_event)
                    custom_timer_event["di_event_list"] = device_state_di_event_list
                    device_state_custom_timer_event_list.append(custom_timer_event)
            current_state_info["custom_timer_event_list"] = device_state_custom_timer_event_list

    else:
        current_state_info = device_current_state.copy()
        recv_datetime = recv_data.get("recv_datetime")
        current_state_info["signal_last_update_datetime"] = recv_datetime
        current_state_info["battery_near_last_update_datetime"] = recv_datetime
        current_state_info["device_abnormality_last_update_datetime"] = recv_datetime
        current_state_info["parameter_abnormality_last_update_datetime"] = recv_datetime
        current_state_info["fw_update_abnormality_last_update_datetime"] = recv_datetime
        current_state_info["di1_last_update_datetime"] = recv_datetime

        if recv_data.get("device_type") in ["PJ2", "PJ3"]:
            current_state_info["di2_last_update_datetime"] = recv_datetime
            current_state_info["di3_last_update_datetime"] = recv_datetime
            current_state_info["di4_last_update_datetime"] = recv_datetime
            current_state_info["di5_last_update_datetime"] = recv_datetime
            current_state_info["di6_last_update_datetime"] = recv_datetime
            current_state_info["di7_last_update_datetime"] = recv_datetime
            current_state_info["di8_last_update_datetime"] = recv_datetime
            current_state_info["do1_last_update_datetime"] = recv_datetime
            current_state_info["do2_last_update_datetime"] = recv_datetime

            """
            if recv_data.get("device_type") == "PJ3":
                current_state_info["ai1_last_update_datetime"] = recv_datetime
                current_state_info["ai2_last_update_datetime"] = recv_datetime
                current_state_info["ai1_threshold_last_update_datetime"] = recv_datetime
                current_state_info["ai2_threshold_last_update_datetime"] = recv_datetime
            """

    return current_state_info


def updateCurrentStateInfo(current_state_info, event_info, event_datetime, recv_data):
    di_state = [
        "di1_state",
        "di2_state",
        "di3_state",
        "di4_state",
        "di5_state",
        "di6_state",
        "di7_state",
        "di8_state",
    ]
    di_change_datetime = [
        "di1_last_change_datetime",
        "di2_last_change_datetime",
        "di3_last_change_datetime",
        "di4_last_change_datetime",
        "di5_last_change_datetime",
        "di6_last_change_datetime",
        "di7_last_change_datetime",
        "di8_last_change_datetime",
    ]
    do_state = ["do1_state", "do2_state"]
    do_change_datetime = ["do1_last_change_datetime", "do2_last_change_datetime"]

    # イベント判定結果をもとに現状態情報を更新
    # 接点入力部
    if event_info.get("event_type") == "di_change":
        terminal_no = event_info.get("terminal_no")
        list_num = int(terminal_no) - 1
        state_key = di_state[list_num]
        change_datetime_key = di_change_datetime[list_num]
        current_state_info[state_key] = event_info.get("di_state")
        current_state_info[change_datetime_key] = event_datetime

        # カスタムタイマーイベント
        recv_datetime = recv_data.get("recv_datetime")
        custom_timer_event_list = current_state_info.get("custom_timer_event_list") or []
        for custom_timer_event in custom_timer_event_list:
            elapsed_time = custom_timer_event.get("elapsed_time") * 60 * 1000
            di_event_list = custom_timer_event.get("di_event_list", [])
            for di_event in di_event_list:
                if di_event.get("di_no") == event_info.get("terminal_no"):
                    if ((di_event.get("di_state") in [0, 1] and di_event.get("di_state") == event_info.get("di_state")) or
                     (di_event.get("di_state") == 2)):
                        di_event["event_judge_datetime"] = event_datetime
                        if event_datetime + elapsed_time < recv_datetime:
                            # カスタムイベント判定日時が受信日時よりも過去の場合、受信日時の30分後を設定
                            di_event["event_datetime"] = recv_datetime + 30 * 60 * 1000
                            di_event["event_hpn_datetime"] = event_datetime + elapsed_time
                            di_event["delay_flag"] = 1
                        else:
                            di_event["event_datetime"] = event_datetime + elapsed_time
                            di_event["message_type"] = recv_data.get("message_type")
                            if recv_data.get("message_type") in ["0011", "0012"]:
                                # 現状態通知（電源ON、定時送信）
                                di_event["event_hpn_datetime"] = event_datetime
                            else:
                                # 状態変化通知
                                di_event["event_hpn_datetime"] = event_datetime + elapsed_time
                    else:
                        di_event["event_judge_datetime"] = 0
                        di_event["event_hpn_datetime"] = 0
                        di_event["event_datetime"] = 0
                        di_event["delay_flag"] = 0
                    break

    # 接点出力部
    elif event_info.get("event_type") == "do_change":
        list_num = int(event_info.get("terminal_no")) - 1
        state_key = do_state[list_num]
        change_datetime_key = do_change_datetime[list_num]
        current_state_info[state_key] = event_info.get("do_state")
        current_state_info[change_datetime_key] = event_datetime

    # デバイス状態（バッテリーニアエンド）
    elif event_info.get("event_type") == "battery_near":
        current_state_info["battery_near_state"] = event_info.get("occurrence_flag")
        current_state_info["battery_near_last_change_datetime"] = event_datetime

    # デバイス状態（機器異常）
    elif event_info.get("event_type") == "device_abnormality":
        current_state_info["device_abnormality"] = event_info.get("occurrence_flag")
        current_state_info["device_abnormality_last_change_datetime"] = event_datetime

    # デバイス状態（パラメータ異常）
    elif event_info.get("event_type") == "parameter_abnormality":
        current_state_info["parameter_abnormality"] = event_info.get("occurrence_flag")
        current_state_info["parameter_abnormality_last_change_datetime"] = event_datetime

    # デバイス状態（FW更新異常）
    elif event_info.get("event_type") == "fw_update_abnormality":
        current_state_info["fw_update_abnormality"] = event_info.get("occurrence_flag")
        current_state_info["fw_update_abnormality_last_change_datetime"] = event_datetime

    # 電波状態
    elif event_info.get("event_type") == "signal_state":
        current_state_info["signal_state"] = event_info.get("signal_state")
        current_state_info["signal_last_change_datetime"] = event_datetime

    return current_state_info


def signalStateJedge(rssi, sinr):
    signal_state_matrix = [
        ["high", "mid", "low", "no_signal"],
        ["mid", "mid", "low", "no_signal"],
        ["low", "low", "low", "no_signal"],
        ["no_signal", "no_signal", "no_signal", "no_signal"],
    ]

    # RSSI判定
    if RSSI_HIGH_MIN <= rssi <= RSSI_HIGH_MAX:
        rssi_revel = SIGNAL_HIGH
    elif RSSI_MID_MIN <= rssi <= RSSI_MID_MAX:
        rssi_revel = SIGNAL_MID
    elif RSSI_LOW_MIN <= rssi <= RSSI_LOW_MAX:
        rssi_revel = SIGNAL_LOW
    else:
        rssi_revel = NO_SIGNAL

    # SINR判定
    if SINR_HIGH_MIN <= sinr <= SINR_HIGH_MAX:
        sinr_revel = SIGNAL_HIGH
    elif SINR_MID_MIN <= sinr <= SINR_MID_MAX:
        sinr_revel = SIGNAL_MID
    elif SINR_LOW_MIN <= sinr <= SINR_LOW_MAX:
        sinr_revel = SIGNAL_LOW
    else:
        sinr_revel = NO_SIGNAL

    signl_state = signal_state_matrix[sinr_revel][rssi_revel]

    return signl_state


def eventJudge(
    recv_data,
    device_current_state,
    device_info,
    device_relation_table,
    group_table,
    remote_control_table,
):

    # 履歴リスト作成
    hist_list = []
    event_datetime = recv_data.get("event_datetime")

    # 現状態設定
    init_state_flg = False
    if device_current_state is None or len(device_current_state) == 0:
        init_state_flg = True
    current_state_info = initCurrentStateInfo(
        recv_data, device_current_state, device_info, init_state_flg
    )
    logger.debug(f"init_state_flg={init_state_flg}")

    # 接点入力変化判定
    if recv_data.get("message_type") in ["0001", "0011", "0012"]:
        event_info = {}
        event_info["event_type"] = "di_change"
        event_info["event_datetime"] = recv_data.get("event_datetime")
        di_list = list(reversed(list(recv_data.get("di_state", []))))
        if recv_data.get("message_type") == "0001":
            di_trigger = recv_data.get("di_trigger")
        di_range = 1 if recv_data.get("device_type") == "PJ1" else 8
        for i in range(di_range):
            event_info["terminal_no"] = i + 1
            event_info["di_state"] = int(di_list[i]) if di_list else None
            if recv_data.get("message_type") == "0001" and event_info["terminal_no"] == di_trigger:
                hist_list_data = createHistListData(
                    recv_data, device_info, event_info, device_relation_table, group_table
                )
                hist_list.append(hist_list_data)
            if not init_state_flg:
                terminal_key = "di" + str(i + 1) + "_state"
                current_di = device_current_state.get(terminal_key)
            if (init_state_flg) or (not init_state_flg and int(di_list[i]) != current_di):
                current_state_info = updateCurrentStateInfo(
                    current_state_info, event_info, event_datetime, recv_data
                )

    # 接点出力変化判定
    if recv_data.get("message_type") in ["0001"] and recv_data.get("device_type") in [
        "PJ2",
        "PJ3",
    ]:
        event_info = {}
        event_info["event_type"] = "do_change"
        event_info["event_datetime"] = recv_data.get("event_datetime")
        do_list = list(reversed(list(recv_data.get("do_state", []))))
        do_trigger = recv_data.get("do_trigger")
        for i in range(2):
            event_info["terminal_no"] = i + 1
            event_info["do_state"] = int(do_list[i]) if do_list else None
            if event_info["terminal_no"] == do_trigger:
                hist_list_data = createHistListData(
                    recv_data, device_info, event_info, device_relation_table, group_table
                )
                hist_list.append(hist_list_data)
            if not init_state_flg:
                terminal_key = "do" + str(i + 1) + "_state"
                current_do = device_current_state.get(terminal_key)
            if (init_state_flg) or (not init_state_flg and int(do_list[i]) != current_do):
                current_state_info = updateCurrentStateInfo(
                    current_state_info, event_info, event_datetime, recv_data
                )

    # バッテリーニアエンド判定
    if recv_data.get("message_type") in ["0001", "0011", "0012"]:
        check_digit = 0b00000001
        event_info = {}
        event_info["event_type"] = "battery_near"
        event_info["event_datetime"] = recv_data.get("event_datetime")
        if (recv_data.get("device_state") & check_digit) == check_digit:
            battery_near_state = 1
        else:
            battery_near_state = 0
        if not init_state_flg:
            current_battery_near = device_current_state.get("battery_near_state")

        if (init_state_flg) or (not init_state_flg and battery_near_state != current_battery_near):
            event_info["occurrence_flag"] = battery_near_state
            if not (init_state_flg == 1 and event_info["occurrence_flag"] == 0):
                hist_list_data = createHistListData(
                    recv_data, device_info, event_info, device_relation_table, group_table
                )
                hist_list.append(hist_list_data)
                current_state_info = updateCurrentStateInfo(
                    current_state_info, event_info, event_datetime, recv_data
                )

    # 機器異常判定
    if recv_data.get("message_type") in ["0001", "0011", "0012"]:
        check_digit = 0b00000100
        event_info = {}
        event_info["event_type"] = "device_abnormality"
        event_info["event_datetime"] = recv_data.get("event_datetime")
        if (recv_data.get("device_state") & check_digit) == check_digit:
            device_abnormality_state = 1
        else:
            device_abnormality_state = 0
        if not init_state_flg:
            current_device_abnormality = device_current_state.get("device_abnormality")

        if (init_state_flg) or (
            not init_state_flg and device_abnormality_state != current_device_abnormality
        ):
            event_info["occurrence_flag"] = device_abnormality_state
            if not (init_state_flg == 1 and event_info["occurrence_flag"] == 0):
                hist_list_data = createHistListData(
                    recv_data, device_info, event_info, device_relation_table, group_table
                )
                hist_list.append(hist_list_data)
                current_state_info = updateCurrentStateInfo(
                    current_state_info, event_info, event_datetime, recv_data
                )

    # パラメータ異常判定
    if recv_data.get("message_type") in ["0001", "0011", "0012"]:
        check_digit = 0b01000000
        event_info = {}
        event_info["event_type"] = "parameter_abnormality"
        event_info["event_datetime"] = recv_data.get("event_datetime")
        if (recv_data.get("device_state") & check_digit) == check_digit:
            parameter_abnormality_state = 1
        else:
            parameter_abnormality_state = 0
        if not init_state_flg:
            current_parameter_abnormality = device_current_state.get("parameter_abnormality")

        if (init_state_flg) or (
            not init_state_flg and parameter_abnormality_state != current_parameter_abnormality
        ):
            event_info["occurrence_flag"] = parameter_abnormality_state
            if not (init_state_flg == 1 and event_info["occurrence_flag"] == 0):
                hist_list_data = createHistListData(
                    recv_data, device_info, event_info, device_relation_table, group_table
                )
                hist_list.append(hist_list_data)
                current_state_info = updateCurrentStateInfo(
                    current_state_info, event_info, event_datetime, recv_data
                )

    # FW更新異常判定
    if recv_data.get("message_type") in ["0001", "0011", "0012"]:
        check_digit = 0b10000000
        event_info = {}
        event_info["event_type"] = "fw_update_abnormality"
        event_info["event_datetime"] = recv_data.get("event_datetime")
        if (recv_data.get("device_state") & check_digit) == check_digit:
            fw_update_abnormality_state = 1
        else:
            fw_update_abnormality_state = 0
        if not init_state_flg:
            current_fw_update_abnormality = device_current_state.get("fw_update_abnormality")

        if (init_state_flg) or (
            not init_state_flg and fw_update_abnormality_state != current_fw_update_abnormality
        ):
            event_info["occurrence_flag"] = fw_update_abnormality_state
            if not (init_state_flg == True and event_info["occurrence_flag"] == 0):
                hist_list_data = createHistListData(
                    recv_data, device_info, event_info, device_relation_table, group_table
                )
                hist_list.append(hist_list_data)
                current_state_info = updateCurrentStateInfo(
                    current_state_info, event_info, event_datetime, recv_data
                )

    # 電源ON
    if recv_data.get("message_type") in ["0011"]:
        event_info = {}
        event_info["event_type"] = "power_on"
        event_info["event_datetime"] = recv_data.get("event_datetime")
        hist_list_data = createHistListData(
            recv_data, device_info, event_info, device_relation_table, group_table
        )
        hist_list.append(hist_list_data)
        current_state_info = updateCurrentStateInfo(current_state_info, event_info, event_datetime, recv_data)

    # 電波状態
    if recv_data.get("message_type") in ["0001", "0011", "0012"]:
        hist_signal_state = signalStateJedge(recv_data.get("rssi"), recv_data.get("sinr"))
        event_info = {}
        event_info["event_type"] = "signal_state"
        event_info["event_datetime"] = recv_data.get("event_datetime")
        event_info["signal_state"] = hist_signal_state
        if (init_state_flg) or (
            not init_state_flg and hist_signal_state != device_current_state.get("signal_state")
        ):
            current_state_info = updateCurrentStateInfo(
                current_state_info, event_info, event_datetime, recv_data
            )

    # 遠隔制御（接点出力制御応答）
    if recv_data.get("message_type") in ["8002"] and recv_data.get("device_type") in [
        "PJ2",
        "PJ3",
    ]:
        event_info = {}
        remote_control_info = ddb.get_remote_control_info(
            recv_data.get("device_req_no"), remote_control_table
        )
        if "control_result" in remote_control_info:
            logger.debug("制御結果記録済みの為、履歴一覧未記録")
            return hist_list, current_state_info
        logger.debug(f"remote_control_info={remote_control_info}")
        event_info["event_datetime"] = remote_control_info.get("req_datetime")
        event_info["do_no"] = remote_control_info.get("do_no")
        event_info["control_trigger"] = remote_control_info.get("control_trigger")
        if event_info["control_trigger"] == "manual_control":
            event_info["control_exec_user_name"] = remote_control_info.get(
                "control_exec_user_name"
            )
            event_info["control_exec_user_email_address"] = remote_control_info.get(
                "control_exec_user_email_address"
            )
        event_info["event_type"] = remote_control_info.get("control_trigger")
        event_info["device_req_no"] = recv_data.get("device_req_no")
        if recv_data.get("control_result") == "0":
            if (
                remote_control_info is None
                or "link_di_no" in remote_control_info
                and remote_control_info["link_di_no"] != 0
            ):
                event_info["control_result"] = "not_excuted_link"
            else:
                event_info["control_result"] = "success"
        else:
            event_info["control_result"] = "failure"

        # 制御トリガー判定
        if event_info["control_trigger"] in [
            "on_timer_control",
            "off_timer_control",
            "timer_control",
        ]:
            event_info["timer_time"] = remote_control_info.get("timer_time")
        if event_info["control_trigger"] in [
            "automation_control",
            "off_automation_control",
            "on_automation_control",
        ]:
            event_info["automation_trigger_device_name"] = remote_control_info.get(
                "automation_trigger_device_name"
            )
            event_info["automation_trigger_imei"] = remote_control_info.get(
                "automation_trigger_imei"
            )
            event_info["automation_trigger_event_type"] = remote_control_info.get(
                "automation_trigger_event_type"
            )
            event_info["automation_trigger_terminal_no"] = remote_control_info.get(
                "automation_trigger_terminal_no"
            )
            event_info["automation_trigger_event_detail_state"] = remote_control_info.get(
                "automation_trigger_event_detail_state"
            )
            event_info["automation_trigger_event_detail_flag"] = remote_control_info.get(
                "automation_trigger_event_detail_flag"
            )

        hist_list_data = createHistListData(
            recv_data, device_info, event_info, device_relation_table, group_table
        )
        hist_list.append(hist_list_data)

    # 遠隔制御（状態変化通知）
    if recv_data.get("message_type") in ["0001"] and recv_data.get("device_type") in [
        "PJ2",
        "PJ3",
    ]:
        event_info = {}
        di_trigger = recv_data.get("di_trigger")
        if di_trigger != 0:
            remote_control_info = ddb.get_remote_control_info_by_device_id(
                device_info.get("device_id"),
                recv_data.get("recv_datetime"),
                remote_control_table,
                di_trigger,
            )
            logger.debug(f"remote_control_info={remote_control_info}")
            if remote_control_info is not None:
                event_info["event_datetime"] = remote_control_info.get("req_datetime")
                event_info["do_no"] = remote_control_info.get("do_no")
                if remote_control_info.get("control_trigger") == "manual_control":
                    event_info["control_exec_user_name"] = remote_control_info.get(
                        "control_exec_user_name"
                    )
                    event_info["control_exec_user_email_address"] = remote_control_info.get(
                        "control_exec_user_email_address"
                    )
                event_info["link_di_no"] = remote_control_info.get("link_di_no")
                di_list = list(reversed(list(recv_data.get("di_state"))))
                event_info["di_state"] = int(di_list[di_trigger - 1])
                event_info["device_req_no"] = remote_control_info.get("device_req_no")
                event_info["control_trigger"] = remote_control_info.get("control_trigger")
                event_info["event_type"] = remote_control_info.get("control_trigger")
                if remote_control_info.get("control_result") == "0":
                    event_info["control_result"] = "success"
                else:
                    event_info["control_result"] = "failure"

                # 制御トリガー判定
                if remote_control_info.get("control_trigger") in [
                    "on_timer_control",
                    "off_timer_control",
                    "timer_control",
                ]:
                    event_info["timer_time"] = remote_control_info.get("timer_time")
                if event_info["control_trigger"] in [
                    "automation_control",
                    "off_automation_control",
                    "on_automation_control",
                ]:
                    event_info["automation_trigger_device_name"] = remote_control_info.get(
                        "automation_trigger_device_name"
                    )
                    event_info["automation_trigger_imei"] = remote_control_info.get(
                        "automation_trigger_imei"
                    )
                    event_info["automation_trigger_event_type"] = remote_control_info.get(
                        "automation_trigger_event_type"
                    )
                    event_info["automation_trigger_terminal_no"] = remote_control_info.get(
                        "automation_trigger_terminal_no"
                    )
                    event_info["automation_trigger_event_detail_state"] = remote_control_info.get(
                        "automation_trigger_event_detail_state"
                    )
                    event_info["automation_trigger_event_detail_flag"] = remote_control_info.get(
                        "automation_trigger_event_detail_flag"
                    )
                hist_list_data = createHistListData(
                    recv_data, device_info, event_info, device_relation_table, group_table
                )
                hist_list.append(hist_list_data)

                # 接点入力状態変化通知結果更新
                ddb.update_control_res_link_di_result(
                    remote_control_info.get("device_req_no"),
                    remote_control_info.get("req_datetime"),
                    remote_control_table,
                )

    return hist_list, current_state_info
