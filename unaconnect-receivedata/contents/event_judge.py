from aws_lambda_powertools import Logger
import ddb
from decimal import Decimal

logger = Logger()

def eventJudge(req_body,device_current_state,device_id,signal_state):
    
    # 現状態設定
    if device_current_state is None or len(device_current_state) == 0:
        device_current_state = initCurrentStateInfo(req_body,device_current_state,device_id,signal_state)
    else:
        device_current_state["unatag_last_recv_datetime"] = req_body.get("timestamp") * 1000
        if req_body.get("dataType") == "GEOLOC":
            if device_current_state.get("latitude_state") != Decimal(str(req_body.get("data").get("lat"))):
                device_current_state["latitude_last_change_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["latitude_state"] = req_body.get("data").get("lat",0)
            if device_current_state.get("longitude_state") != Decimal(str(req_body.get("data").get("lng"))):
                device_current_state["longitude_last_change_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["longitude_state"] = req_body.get("data").get("lng",0)
            if device_current_state.get("precision_state") != Decimal(str(req_body.get("data").get("radius",0))):
                device_current_state["precision_last_change_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["precision_state"] = req_body.get("data").get("radius")
            device_current_state["latitude_last_update_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["longitude_last_update_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["precision_last_update_datetime"] = req_body.get("timestamp") * 1000
        elif req_body.get("dataType") == "DATA":
            if device_current_state.get("battery_voltage") != Decimal(str(req_body.get("batteryVoltage"))):
                device_current_state["battery_near_last_change_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["battery_voltage"] = req_body.get("batteryVoltage")
            device_current_state["battery_near_last_update_datetime"] = req_body.get("timestamp") * 1000
        elif req_body.get("dataType") == "TELEMETRY":
            if device_current_state.get("signal_state") != signal_state:
                device_current_state["signal_last_change_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["signal_state"] = signal_state
            device_current_state["signal_last_update_datetime"] = req_body.get("timestamp") * 1000
    logger.debug(f"device_current_state={device_current_state}")   
    return device_current_state


def initCurrentStateInfo(req_body,device_current_state,device_id,signal_state):
    device_current_state = {}
    device_current_state["unatag_last_recv_datetime"] = req_body.get("timestamp") * 1000
    if req_body.get("dataType") == "GEOLOC":
        device_current_state["device_id"] = device_id
        device_current_state["latitude_state"] = req_body.get("data").get("lat",0)
        device_current_state["longitude_state"] = req_body.get("data").get("lng",0)
        device_current_state["precision_state"] = req_body.get("data").get("radius",0)
        device_current_state["latitude_last_update_datetime"] = req_body.get("timestamp") * 1000
        device_current_state["longitude_last_update_datetime"] = req_body.get("timestamp") * 1000
        device_current_state["precision_last_update_datetime"] = req_body.get("timestamp") * 1000
        device_current_state["latitude_last_change_datetime"] = req_body.get("timestamp") * 1000
        device_current_state["longitude_last_change_datetime"] = req_body.get("timestamp") * 1000
        device_current_state["precision_last_change_datetime"] = req_body.get("timestamp") * 1000
    elif req_body.get("dataType") == "DATA":
        device_current_state["device_id"] = device_id
        device_current_state["battery_voltage"] = req_body.get("batteryVoltage",0)
        device_current_state["battery_near_last_update_datetime"] = req_body.get("timestamp") * 1000
        device_current_state["battery_near_last_change_datetime"] = req_body.get("timestamp") * 1000
    elif req_body.get("dataType") == "TELEMETRY":
        device_current_state["device_id"] = device_id
        device_current_state["signal_state"] = signal_state
        device_current_state["signal_last_change_datetime"] = req_body.get("timestamp") * 1000
        device_current_state["signal_last_update_datetime"] = req_body.get("timestamp") * 1000

    return device_current_state


def judge_near_battery(current_state_info,hist_item,hist_list_table):

    current_battry_voltage = current_state_info.get("battery_voltage")
    current_battry_state = current_state_info.get("battery_near_state")

    if current_battry_state:
        if (current_battry_state == 0) and (current_battry_voltage < 2.0):
            current_state_info["battery_near_state"] = 1
            hist_item["hist_data"]["occurrence_flag"] = 1
            ddb.put_db_item(hist_item,hist_list_table)
        elif (current_battry_state == 1) and (current_battry_voltage >= 3.0):
            current_state_info["battery_near_state"] = 0
            hist_item["hist_data"]["occurrence_flag"] = 0
            ddb.put_db_item(hist_item,hist_list_table)
    else:
        if current_battry_voltage < 2.0:
            current_state_info["battery_near_state"] = 1
            hist_item["hist_data"]["occurrence_flag"] = 1
            ddb.put_db_item(hist_item,hist_list_table)
        else:
            current_state_info["battery_near_state"] = 0
            
    return current_state_info


def judge_signal_state(signal_score):

    if signal_score >= 60.0:
        signal_state = "high"
    elif signal_score >= 40.0:
        signal_state = "mid"
    else:
        signal_state = "low"

    return signal_state