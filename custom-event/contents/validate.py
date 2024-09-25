import db

from aws_lambda_powertools import Logger

logger = Logger()

def validate(event_trigger, contract_id, event_datetime):
    logger.debug("validate開始")

    # イベントトリガー
    if event_trigger not in ["lambda-custom-event-trigger"]:
        logger.debug("イベントトリガー不正")
        return -1

    # 契約ID
    if contract_id is None:
        logger.debug("契約ID未指定")
        return -1

    # イベント日時
    if event_datetime is None:
        logger.debug("契約ID未指定")
        return -1

    logger.debug("validate終了")
    return 0
