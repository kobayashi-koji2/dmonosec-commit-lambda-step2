import json
import ddb
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
    if "authorization" not in headers or "device_id" not in pathParam:
        return {"message": "リクエストパラメータが不正です。"}

    device_id = event["pathParameters"]["device_id"]
    if not device_id:
        return {"message": "リクエストパラメータが不正です。"}
    body = json.loads(body)
    logger.info(f"device_id: {device_id}")
    logger.info(f"body: {body}")

    # 1.3 ユーザー権限確認
    contract_info = db.get_contract_info(
        user_info["contract_id"], tables["contract_table"]
    )
    if not contract_info:
        return {"message": "アカウント情報が存在しません。"}

    # 1.4 デバイス種別取得
    device_info = ddb.get_device_info(device_id, tables["device_table"])
    logger.info(f"device_id: {device_id}")
    logger.info(f"device_info: {device_info}")
    if len(device_info) == 0:
        return {"message": "デバイス情報が存在しません。"}
    elif len(device_info) >= 2:
        return {
            "message": "デバイスIDに「契約状態:初期受信待ち」「契約状態:使用可能」の機器が複数紐づいています"
        }
    device_type = device_info[0]["device_type"]

    # 1.5 デバイス種別チェック
    if device_type == "UnaTag":
        return {"message": "UnaTagに接点入力設定を行うことはできません。"}

    ##################
    # 2 デバイス操作権限チェック
    ##################
    operation_auth = operation_auth_check(user_info, contract_info, device_id, tables)
    if not operation_auth:
        return {"message": "不正なデバイスIDが指定されています。"}
    # 端子設定チェック
    terminal = terminal_check(body, device_id, device_type, tables)
    if not terminal:
        return {"message": "デバイス種別と端子設定が一致しません。"}

    input = input_check(body)
    if not input:
        return {"message": "入力パラメータが不正です。"}

    # 接点入力_未変化検出_単位と接点入力_未変化検出_期間の整合性をチェック
    # ※接点入力_未変化検出_単位が空文字列の場合、接点入力_未変化検出_期間を 0 でリセットする
    is_di_healthy_data_ok = di_healthy_data_check_and_reset(body)
    if not is_di_healthy_data_ok:
        return {
            "message": "接点入力_未変化検出_単位と接点入力_未変化検出_期間が整合していません。"
        }
    return {"device_id": device_id, "imei": device_info[0]["imei"], "body": body}


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


# 端子設定チェック
def terminal_check(body, device_id, device_type, tables):
    di = len(body.get("di_list", {}))
    di_no_list = []
    # デバイス種別と端子数
    if (
        (device_type == "PJ1" and di == 1)
        or (device_type == "PJ2" and di == 8)
        or (device_type == "PJ3" and di == 8)
    ):
        # 端子番号
        for item in body.get("di_list", {}):
            if not isinstance(item.get("di_no"), int):
                return False
            di_no_list.append(item.get("di_no"))
        # 端子番号の範囲
        if all(1 <= int(num) <= di for num in di_no_list):
            # 端子番号の重複
            if len(set(di_no_list)) == len(di_no_list):
                return True
    return False


# 接点入力_未変化検出_単位と接点入力_未変化検出_期間の整合性をチェック
# ※接点入力_未変化検出_単位が空文字列の場合、接点入力_未変化検出_期間を 0 でリセットする
def di_healthy_data_check_and_reset(body):
    for item in body.get("di_list", []):
        if "di_healthy_type" in item and "di_healthy_period" in item:
            if item["di_healthy_type"] == "":
                item["di_healthy_period"] = 0
            elif (
                item["di_healthy_type"] == "day"
                and 1 <= float(item["di_healthy_period"]) <= 100
            ):
                # 接点入力_未変化検出_単位が "day" の場合、接点入力_未変化検出_期間は 1-100 の範囲のみ許可
                continue
            elif (
                item["di_healthy_type"] == "hour"
                and 1 <= float(item["di_healthy_period"]) <= 23
            ):
                # 接点入力_未変化検出_単位が "hour" の場合、接点入力_未変化検出_期間は 1-23 の範囲のみ許可
                continue
            else:
                return False
        elif "di_healthy_type" in item:
            if item["di_healthy_type"] == "":
                item["di_healthy_period"] = 0
            else:
                return False
        elif "di_healthy_period" in item:
            return False
        else:
            # 接点入力_未変化検出_単位と接点入力_未変化検出_期間がいずれも指定されていない場合、スキップ
            continue
    return True


# 入力チェック
# 画面一覧に記載のされている入力制限のみチェック
def input_check(param):
    out_range_list, invalid_format_list, invalid_data_type_list = [], [], []

    # 文字数の制限
    # 接点名、ON-OFF名は未登録の場合、WEB側で初期値を表示する仕様のため空文字を許容する
    str_value_limits = {
        "di_name": {0, 50},
        "di_on_name": {0, 20},
        "di_on_icon": {1, 30},
        "di_off_name": {0, 20},
        "di_off_icon": {1, 30},
    }

    # 桁数の制限
    int_float_value_limits = {"di_healthy_period": {0, 100}}

    # アイコンコード一覧参照
    icon_list = [
        "on",
        "off",
        "open",
        "close",
        "lock",
        "unlock",
        "on_light",
        "off_light",
        "wave",
    ]

    # 特定の文字列に一致
    str_format = {
        "di_on_icon": icon_list,
        "di_off_icon": icon_list,
        "di_healthy_type": ["", "day", "hour"],
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
                    # 文字列フォーマット
                    if key in str_format and value not in str_format[key]:
                        logger.info(
                            f"Key:{key}  value:{value} - reason:文字列の形式が不正です。"
                        )
                        invalid_format_list.append(key)
                # 数値
                elif isinstance(value, (int, float)):
                    # データ型
                    if key in str_value_limits:
                        logger.info(
                            f"Key:{key}  value:{value} - reason:データ型が不正です。"
                        )
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
