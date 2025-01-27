from aws_lambda_powertools import Logger

# layer
import db

logger = Logger()


# パラメータチェック
def validate(event, user_info, tables):
    pathParam = event.get("pathParameters", {})
    logger.debug(f"pathParam: {pathParam}")
    if not pathParam:
        return {"message": "リクエストパラメータが不正です。"}
    if "device_id" not in pathParam:
        return {"message": "リクエストパラメータが不正です。"}

    device_id = event["pathParameters"]["device_id"]
    logger.debug(f"device_id: {device_id}")

    # 1.3 ユーザー権限確認
    contract_info = db.get_contract_info(user_info["contract_id"], tables["contract_table"])
    if not contract_info:
        return {"message": "アカウント情報が存在しません。"}
    if user_info.get("user_type") != "admin" and user_info.get("user_type") != "sub_admin":
        return {"message": "権限がありません。"}

    ##################
    # 2 デバイス操作権限チェック
    ##################
    device_info = db.get_device_info_other_than_unavailable(device_id, tables["device_table"])
    logger.debug(f"device_id: {device_id}")
    logger.debug(f"device_info: {device_info}")
    if not device_info:
        return {"message": "デバイス情報が存在しません。"}

    operation_auth = operation_auth_check(user_info, contract_info, device_id, tables)
    if not operation_auth:
        return {"message": "不正なデバイスIDが指定されています。"}

    return {"device_id": device_id}


# 操作権限チェック
def operation_auth_check(user_info, contract_info, device_id, tables):
    user_type, user_id = user_info["user_type"], user_info["user_id"]
    # 2.1 デバイスID一覧取得
    accunt_devices = contract_info["contract_data"]["device_list"]
    logger.debug(f"ユーザID:{user_id}")
    logger.debug(f"権限:{user_type}")
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
