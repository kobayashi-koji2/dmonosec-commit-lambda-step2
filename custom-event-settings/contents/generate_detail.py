import uuid
import ddb
import db
import time
import math
import time
from aws_lambda_powertools import Logger

logger = Logger()

# カスタムイベント設定登録
def create_custom_event_info(custom_event_info, device_table, device_id,device_state_table):
    device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
    device_state = db.get_device_state(device_id, device_state_table)
    custom_event_id = str(uuid.uuid4())
    custom_event_reg_datetime = math.floor(time.time())
    custom_put_item = dict()

    ### 既存のカスタムイベントリストをコピー
    custom_event_list = device_info.get("device_data").get("config").get("custom_event_list", [])

    ### 新規カスタムイベント設定情報を作成
    # 日時指定
    if custom_event_info["event_type"] == 0:
        if not custom_event_info["custom_event_name"]:
            custom_event_name = "無題の日時カスタムイベント"
        else:
            custom_event_name = custom_event_info["custom_event_name"]
        custom_put_item = {
            "custom_event_id": custom_event_id,
            'custom_event_reg_datetime': custom_event_reg_datetime,
            "event_type": custom_event_info["event_type"],
            "custom_event_name": custom_event_name,
            "time": custom_event_info["time"],
            "weekday": custom_event_info["weekday"],
            "di_event_list": custom_event_info["di_event_list"],
        }

    # 状態継続
    elif custom_event_info["event_type"] == 1:
        if not custom_event_info["custom_event_name"]:
            custom_event_name = "無題の継続時間カスタムイベント"
        else:
            custom_event_name = custom_event_info["custom_event_name"]
        custom_put_item = {
            "custom_event_id": custom_event_id,
            'custom_event_reg_datetime': custom_event_reg_datetime,
            "event_type": custom_event_info["event_type"],
            "custom_event_name": custom_event_name,
            "elapsed_time": custom_event_info["elapsed_time"],
            "di_event_list": custom_event_info["di_event_list"],
        }

    custom_event_list.append(custom_put_item)

    # デバイス管理テーブルにカスタムイベント設定を登録
    imei = device_info["identification_id"]
    custom_event_db_update = ddb.update_ddb_custom_event_info(custom_event_list, device_table, device_id, imei)

    # デバイス現状態にカスタムタイマーイベントリスト追加
    if custom_event_info["event_type"] == 1:
        custom_timer_event_list = list()
        if device_state:
            if len(device_state.get("custom_timer_event_list",[])):
                custom_timer_event_list = device_state.get("custom_timer_event_list")
            di_event_list = list()
            for di_event in custom_event_info["di_event_list"]:
                di_no = di_event["di_no"]
                delay_flag = 0
                event_judge_datetime = custom_event_reg_datetime
                """
                di_last_change_datetime = f"di{di_no}_last_change_datetime"
                last_change_datetime = device_state.get(di_last_change_datetime)
                # 最終変化日時 + 経過時間(分) < 登録日時の場合は3分後のイベント検知
                if last_change_datetime + custom_event_info["elapsed_time"] * 60 * 1000 < custom_event_reg_datetime:
                    delay_flag = 1
                    event_judge_datetime = custom_event_reg_datetime + 3 * 60 * 1000
                """
                di_list_item = {
                    "di_no": di_no,
                    "di_state": di_event["di_state"],
                    "event_judge_datetime": event_judge_datetime,
                    "delay_flag": delay_flag,
                    "di_custom_event_state": 0
                }
                di_event_list.append(di_list_item)
            custom_timer_event_item = {
                "custom_event_id": custom_event_id,
                "elapsed_time": custom_event_info["elapsed_time"],
                "di_event_list" : di_event_list,
            }
            custom_timer_event_list.append(custom_timer_event_item)
            device_state_custom_event_db_update = ddb.update_ddb_device_state_info(custom_timer_event_list, device_state_table, device_id)
        else:
            device_state_custom_event_db_update = True
    else:
        device_state_custom_event_db_update = True
    
    if custom_event_db_update and device_state_custom_event_db_update:
        res_body = {"message": "データの登録に成功しました。"}
        return True, res_body
    else:
        res_body = {"message": "データの登録に失敗しました。"}
        return False, res_body


# カスタムイベント設定更新         
def update_custom_event_info(custom_event_info, device_table, device_id,device_state_table):
    device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
    device_state = db.get_device_state(device_id, device_state_table)
    custom_put_item = dict()
    
    for custom_event in device_info.get("device_data").get("config").get("custom_event_list", []):
        if custom_event["custom_event_id"] == custom_event_info["custom_event_id"]:
            custom_event_reg_datetime = custom_event["custom_event_reg_datetime"]

    # 日時指定
    if custom_event_info["event_type"] == 0:
        if not custom_event_info["custom_event_name"]:
            custom_event_name = "無題の日時カスタムイベント"
        else:
            custom_event_name = custom_event_info["custom_event_name"]
        custom_put_item = {
            "custom_event_id": custom_event_info["custom_event_id"],
            'custom_event_reg_datetime': custom_event_reg_datetime,
            "event_type": custom_event_info["event_type"],
            "custom_event_name": custom_event_name,
            "time": custom_event_info["time"],
            "weekday": custom_event_info["weekday"],
            "di_event_list": custom_event_info["di_event_list"],
        }

    # 状態継続
    elif custom_event_info["event_type"] == 1:
        if not custom_event_info["custom_event_name"]:
            custom_event_name = "無題の継続時間カスタムイベント"
        else:
            custom_event_name = custom_event_info["custom_event_name"]
        custom_put_item = {
            "custom_event_id": custom_event_info["custom_event_id"],
            'custom_event_reg_datetime': custom_event_reg_datetime,
            "event_type": custom_event_info["event_type"],
            "custom_event_name": custom_event_name,
            "elapsed_time": custom_event_info["elapsed_time"],
            "di_event_list": custom_event_info["di_event_list"],
        }

    custom_event_list = list()
    imei = device_info["identification_id"]
    for custom_event in device_info.get("device_data").get("config").get("custom_event_list", []):
        if custom_event["custom_event_id"] == custom_event_info["custom_event_id"]:
            custom_event = custom_put_item
        custom_event_list.append(custom_event)

    di_event_list = []
    if device_state:
        for device_state_custom_event in device_state.get("custom_timer_event_list",[]):
            if custom_event_info["custom_event_id"] != device_state_custom_event["custom_event_id"]:
                continue
            if custom_event_info["elapsed_time"] == device_state_custom_event["elapsed_time"]:
                for di_event in custom_event_info["di_event_list"]:
                    find_flag = False
                    for device_state_di_event in device_state_custom_event["di_event_list"]:
                        if (di_event["di_no"] == device_state_di_event["di_no"] and
                            di_event["di_state"] == device_state_di_event["di_state"]):
                            di_event_list.append(device_state_di_event)
                            find_flag = True
                            break
                    if not find_flag:
                        di_no = di_event["di_no"]
                        delay_flag = 0
                        event_judge_datetime = custom_event_reg_datetime
                        """
                        di_last_change_datetime = f"di{di_no}_last_change_datetime"
                        last_change_datetime = device_state.get(di_last_change_datetime)
                        # 最終変化日時 + 経過時間(分) < 登録日時の場合は3分後のイベント検知
                        if last_change_datetime + custom_event_info["elapsed_time"] * 60 * 1000 < custom_event_reg_datetime:
                            delay_flag = 1
                            event_judge_datetime = custom_event_reg_datetime + 3 * 60 * 1000
                        """
                        di_list_item = {
                            "di_no": di_no,
                            "di_state": di_event["di_state"],
                            "event_judge_datetime": event_judge_datetime,
                            "delay_flag": delay_flag,
                            "di_custom_event_state": 0
                        }
                        di_event_list.append(di_list_item)
                device_state_put_item = {
                    "custom_event_id": custom_event_info["custom_event_id"],
                    "elapsed_time": custom_event_info["elapsed_time"],
                    "di_event_list": di_event_list,
                }
            else:
                di_event_list = list()
                for di_event in custom_event_info["di_event_list"]:
                    di_no = di_event["di_no"]
                    delay_flag = 0
                    event_judge_datetime = custom_event_reg_datetime
                    """
                    di_last_change_datetime = f"di{di_no}_last_change_datetime"
                    last_change_datetime = device_state.get(di_last_change_datetime)
                    # 最終変化日時 + 経過時間(分) < 登録日時の場合は3分後のイベント検知
                    if last_change_datetime + custom_event_info["elapsed_time"] * 60 * 1000 < custom_event_reg_datetime:
                        delay_flag = 1
                        event_judge_datetime = custom_event_reg_datetime + 3 * 60 * 1000
                    """
                    di_list_item = {
                        "di_no": di_no,
                        "di_state": di_event["di_state"],
                        "event_judge_datetime": event_judge_datetime,
                        "delay_flag": delay_flag,
                        "di_custom_event_state": 0
                    }
                    di_event_list.append(di_list_item)
                device_state_put_item = {
                    "custom_event_id": custom_event_info["custom_event_id"],
                    "elapsed_time": custom_event_info["elapsed_time"],
                    "di_event_list" : di_event_list,
                }
            
    # デバイス管理のカスタムイベントリスト更新
    custom_event_db_update = ddb.update_ddb_custom_event_info(custom_event_list, device_table, device_id, imei)
            
    # デバイス現状態のカスタムタイマーイベントリスト更新
    device_state_timer_list = list()
    if device_state:
        for device_state_custom_event in device_state.get("custom_timer_event_list",[]):
            if device_state_custom_event["custom_event_id"] == custom_event_info["custom_event_id"]:
                device_state_custom_event = device_state_put_item
            device_state_timer_list.append(device_state_custom_event)
        device_state_custom_event_db_update = ddb.update_ddb_device_state_info(device_state_put_item, device_state_table, device_id)
    else:
        device_state_custom_event_db_update = True

    if custom_event_db_update == True:
        if device_state_custom_event_db_update == True:
            res_body = {"message": "データの登録に成功しました。"}
            return True, res_body
        elif device_state_custom_event_db_update == False or not device_state_custom_event_db_update:
            res_body = {"message": "データの登録に失敗しました。"}
            return False, res_body
    else:
        res_body = {"message": "データの登録に失敗しました。"}
        return False, res_body
