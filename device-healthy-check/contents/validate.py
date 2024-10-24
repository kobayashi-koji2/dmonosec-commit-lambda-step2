import db

from aws_lambda_powertools import Logger

logger = Logger()

def validate(event_trigger, device_id, di_no, contract_id, event_type, event_datetime, device_table):
    logger.debug("validate開始")

    # イベントトリガー
    if event_trigger not in ["lambda-receivedata-2", "lambda-unaconnect-receivedata", "lambda-device-healthy-check-trigger"]:
        logger.debug("イベントトリガー不正")
        return -1

    if event_trigger in ["lambda-receivedata-2"]:
        if event_type == "device_unhealthy":
            # デバイスID
            if device_id is None:
                logger.debug("デバイスID未指定")
                return -1

            # event_datetime
            if event_datetime is None:
                logger.debug("イベント発生日時未指定")
                return -1

        elif event_type == "di_unhealthy":
            # デバイスID
            if device_id is None:
                logger.debug("デバイスID未指定")
                return -1

            # DI接点番号
            if di_no is None:
                logger.debug("DI接点番号未指定")
                return -1

            # デバイス種別ごとに接点数を判定
            device_info = db.get_device_info_other_than_unavailable(device_id, device_table)
            device_type = device_info["device_type"]
            if device_type == "PJ1":
                max_di_no = 1
            elif device_type == "PJ2":
                max_di_no = 8
            else:
                max_di_no = 0

            if 1 > di_no or di_no > max_di_no:
                logger.debug(f"DI接点番号不正 device_id={device_info.get("device_id")} di_no={di_no}")
                return -1

            # event_datetime
            if event_datetime is None:
                logger.debug("イベント発生日時未指定")
                return -1

        else:
            logger.debug("イベントタイプ不正")
            return -1

    elif event_trigger == "lambda-device-healthy-check-trigger":
        # 契約ID
        if contract_id is None:
            logger.debug("契約ID未指定")
            return -1

    if event_trigger in ["lambda-unaconnect-receivedata"]:
        if event_type == "device_unhealthy":
            # デバイスID
            if device_id is None:
                logger.debug("デバイスID未指定")
                return -1

            # event_datetime
            if event_datetime is None:
                logger.debug("イベント発生日時未指定")
                return -1

        else:
            logger.debug("イベントタイプ不正")
            return -1

    logger.debug("validate終了")
    return 0
