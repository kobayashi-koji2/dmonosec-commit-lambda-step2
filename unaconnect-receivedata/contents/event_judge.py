from aws_lambda_powertools import Logger
import json
import decimal

logger = Logger()

def eventJudge(req_body,device_current_state,device_id):
    
    # 現状態設定
    if device_current_state is None or len(device_current_state) == 0:
        device_current_state = initCurrentStateInfo(req_body,device_current_state,device_id)
    else:
        if req_body.get("dataType") == "GEOLOC":
            if device_current_state.get("latitude_state") != req_body.get("data").get("lat"):
                device_current_state["latitude_last_change_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["latitude_state"] = req_body.get("data").get("lat",0)
            if device_current_state.get("longitude_state") != req_body.get("data").get("lng"):
                device_current_state["longitude_last_change_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["longitude_state"] = req_body.get("data").get("lng",0)
            if device_current_state.get("precision_state") != req_body.get("data").get("radius",0):
                device_current_state["precision_last_change_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["precision_state"] = req_body.get("data").get("radius")
            device_current_state["latitude_last_update_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["longitude_last_update_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["precision_last_update_datetime"] = req_body.get("timestamp") * 1000
        elif req_body.get("dataType") == "DATA":
            if device_current_state.get("battery_voltage") != req_body.get("batteryVoltage"):
                device_current_state["battery_near_last_change_datetime"] = req_body.get("timestamp") * 1000
            device_current_state["battery_voltage"] = req_body.get("batteryVoltage")
            device_current_state["battery_near_last_update_datetime"] = req_body.get("timestamp") * 1000
    logger.debug(f"device_current_state={device_current_state}")   
    return device_current_state


def initCurrentStateInfo(req_body,device_current_state,device_id):
    device_current_state = {}
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
    device_current_state = json.loads(json.dumps(device_current_state), parse_float=decimal.Decimal)  
    return device_current_state