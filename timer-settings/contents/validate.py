import json
import re
from aws_lambda_powertools import Logger

# layer
import db

logger = Logger()


# パラメータチェック
def validate(event, user_info, tables):
    headers = event.get("headers", {})
    pathParam = event.get("pathParameters", {})
    body = event.get("body", {})
    if not headers or not pathParam or not body:
        return {"message": "リクエストパラメータが不正です。"}
    if "Authorization" not in headers or "device_id" not in pathParam:
        return {"message": "リクエストパラメータが不正です。"}

    device_id = event["pathParameters"]["device_id"]
    body = json.loads(body)
    logger.info(f"device_id: {device_id}")
    logger.info(f"body: {body}")

    # 1.3 ユーザー権限確認
    contract_info = db.get_contract_info(user_info["contract_id"], tables["contract_table"])
    if not contract_info:
        return {"message": "アカウント情報が存在しません。"}

    ##################
    # 2 デバイス操作権限チェック
    ##################
    device_info = db.get_device_info_other_than_unavailable(device_id, tables["device_table"])
    logger.info(f"device_id: {device_id}")
    logger.info(f"device_info: {device_info}")
    if not device_info:
        return {"message": "デバイス情報が存在しません。"}

    operation_auth = operation_auth_check(user_info, contract_info, device_id, tables)
    if not operation_auth:
        return {"message": "不正なデバイスIDが指定されています。"}
    # デバイス種別チェック
    device_terminal = device_terminal_check(device_info["device_type"], body)
    if not device_terminal:
        return {"message": "デバイス種別と端子設定が一致しません。"}

    input = input_check(body)
    if not input:
        return {"message": "入力パラメータが不正です。"}

    # 重複チェック
    http_method = event.get("httpMethod")
    if http_method == "POST":
        duplicate = duplicate_check(body, device_info)
        if not duplicate:
            return {"message": "同じ設定が重複しています。"}

    return {"device_id": device_id, "body": body}


# 操作権限チェック
def operation_auth_check(user_info, contract_info, device_id, tables):
    user_type, user_id = user_info["user_type"], user_info["user_id"]
    # 2.1 デバイスID一覧取得
    accunt_devices = contract_info["contract_data"]["device_list"]
    logger.info(f"ユーザID:{user_id}")
    logger.info(f"権限:{user_type}")
    if device_id not in accunt_devices:
        return False

    if user_type == "referrer":
        return False
    if user_type == "worker":
        # 3.1 ユーザに紐づくデバイスID取得
        user_devices = db.get_user_relation_device_id_list(
            user_id, tables["device_relation_table"]
        )
        if device_id not in set(user_devices):
            return False
    return True


# デバイス種別,端子番号チェック
def device_terminal_check(device_type, body):
    if device_type == "PJ2":
        if body.get("do_no") in [1, 2]:
            return True
    return False


# 入力チェック
# 画面一覧に記載のされている入力制限のみチェック
def input_check(param):
    out_range_list, invalid_format_list, invalid_data_type_list = [], [], []

    # 文字数の制限
    str_value_limits = {"do_timer_name": {0, 30}, "do_time": {0, 5}}

    # 桁数の制限
    int_float_value_limits = {"do_onoff_control": {0, 1}}

    # 正規表現
    regex = {
        "do_time": re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$"),  # hh:mm形式 00:00から23:59
        "do_weekday": re.compile(r"^$|^([0-6],)*[0-6]$"),  # 空文字もしくは 1,3,5 のような形式
    }

    # dict型の全要素を探索して入力値をチェック
    def check_dict_value(param):
        if isinstance(param, dict):
            for key, value in param.items():
                # 文字列
                if isinstance(value, str):
                    # データ型
                    if key in int_float_value_limits:
                        min_value, max_value = int_float_value_limits[key]
                        try:
                            if not float(min_value) <= float(value) <= float(max_value):
                                logger.info(
                                    f"Key:{key}  value:{value} - reason:桁数の制限を超えています。"
                                )
                                out_range_list.append(key)
                        except ValueError:
                            logger.info(
                                f"Key:{key}  value:{value} - reason:文字列の形式が不正です。"
                            )
                            invalid_data_type_list.append(key)
                    # 文字数
                    if key in str_value_limits:
                        min_length, max_length = str_value_limits[key]
                        string_length = len(value)
                        if not min_length <= string_length <= max_length:
                            logger.info(
                                f"Key:{key}  value:{value} - reason:文字数の制限を超えています。"
                            )
                            out_range_list.append(key)
                    # 正規表現
                    if key in regex and not regex[key].match(value):
                        logger.info(f"Key:{key}  value:{value} - reason:文字列の形式が不正です。")
                        invalid_format_list.append(key)
                # 数値
                elif isinstance(value, (int, float)):
                    # データ型
                    if key in str_value_limits:
                        logger.info(f"Key:{key}  value:{value} - reason:データ型が不正です。")
                        invalid_data_type_list.append(key)
                    # 桁数
                    if key in int_float_value_limits:
                        min_value, max_value = int_float_value_limits[key]
                        if not float(min_value) <= float(value) <= float(max_value):
                            logger.info(
                                f"Key:{key}  value:{value} - reason:桁数の制限を超えています。"
                            )
                            out_range_list.append(key)
                else:
                    check_dict_value(value)
        elif isinstance(param, list):
            for item in param:
                check_dict_value(item)
        return out_range_list

    out_range_list = check_dict_value(param)
    if (
        len(out_range_list) == 0
        and len(invalid_format_list) == 0
        and len(invalid_data_type_list) == 0
    ):
        return True
    return False


# 重複チェック
def duplicate_check(param, device_info):
    do_list = device_info.get("device_data").get("config").get("terminal_settings").get("do_list",[])
    for do in do_list:
        if do.get("do_no") != param.get("do_no"):
            continue
        do_timer_list = do.get("do_timer_list", [])

        for do_timer in do_timer_list:
            do_weekday = set(do_timer.get("do_weekday").split(","))
            param_do_weekday = set(param.get("do_weekday").split(","))
            if (
                do_timer.get("do_time") == param.get("do_time")
                and len(list(do_weekday & param_do_weekday)) > 0
            ):
                return False
    return True
