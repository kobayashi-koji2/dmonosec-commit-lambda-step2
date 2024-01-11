import os
import ddb
from aws_lambda_powertools import Logger

logger = Logger()

RECV_PAST_TIME = int(os.environ["RECV_PAST_TIME"])
RECV_FUTURE_TIME = int(os.environ["RECV_FUTURE_TIME"])


def getByteArray(Payload, index, len):
    start = index[0]
    end = index[0] + len
    index[0] += len
    return Payload[start:end]


def validate(Payload, Simid, RecvDatetime, hist_table):
    logger.debug("validate開始")
    index = [0]
    nlen = int.from_bytes(getByteArray(Payload, index, 2), "big")
    nDeviceType = int.from_bytes(getByteArray(Payload, index, 2), "big")
    nVer = int.from_bytes(getByteArray(Payload, index, 2), "big")
    nMsgType = int.from_bytes(getByteArray(Payload, index, 2), "big")
    if nMsgType == 0x8002:
        szReqNo = getByteArray(Payload, index, 4).hex()
    else:
        szReqNo = None
    nEventTime = int.from_bytes(getByteArray(Payload, index, 8), "big")

    # メッセージ長
    if (nlen != len(Payload)) or (len(Payload) < 8):
        logger.debug("メッセージ長エラー")
        return 1

    # デバイス種別
    if nDeviceType not in [0x0001, 0x0002, 0x0003]:
        logger.debug("デバイス種別エラー")
        return 2

    # メッセージ種別
    if nMsgType not in [0x0001, 0x0011, 0x0012, 0x8002]:
        logger.debug("メッセージ種別エラー")
        return 3

    # イベント発生日時
    if not ((RecvDatetime - RECV_PAST_TIME) <= nEventTime <= (RecvDatetime + RECV_FUTURE_TIME)):
        logger.debug(f"RecvDatetime={RecvDatetime}, nEventTime={nEventTime}")
        logger.debug("イベント発生日時エラー")
        return 4

    # 同一電文
    count = ddb.get_history_count(Simid, nEventTime, hist_table)
    # logger.debug(f'Simid={Simid}, nEventTime={nEventTime}, count={count}')
    logger.debug(f"Simid={Simid}, nEventTime={nEventTime}, count={count}")
    if count != 0:
        logger.debug("受信済みデータエラー")
        return 5

    logger.debug("validate終了")
    return 0
