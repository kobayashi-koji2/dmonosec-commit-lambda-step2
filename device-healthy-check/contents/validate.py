from aws_lambda_powertools import Logger

logger = Logger()

def validate(event_trigger, device_id, di_no, contract_id, event_type, event_datetime):
    logger.debug("validate開始")

    # イベントトリガー
    if event_trigger not in ["lambda-receivedata-2", "lambda_device_settings", "lambda-device-healthy-check-trigger"]:
        logger.debug("イベントトリガー不正")
        return -1

    if event_trigger in ["lambda-receivedata-2", "lambda_device_settings"]:
        if event_type == "device_unhealthy":
            # デバイスID
            if device_id is None:
                logger.debug("デバイスID未指定")
                return -1

            # event_datetime
            if event_datetime is None:
                logger.debug("イベント発生日時未指定")
                return -1

        if event_type == "di_unhealthy":
            # デバイスID
            if device_id is None:
                logger.debug("デバイスID未指定")
                return -1

            # DI接点番号
            if di_no is None:
                logger.debug("DI接点番号未指定")
                return -1

            # event_datetime
            if event_datetime is None:
                logger.debug("イベント発生日時未指定")
                return -1

    elif event_trigger == "lambda-device-healthy-check-trigger":
        # 契約ID
        if contract_id is None:
            logger.debug("契約ID未指定")
            return -1

    logger.debug("validate終了")
    return 0
