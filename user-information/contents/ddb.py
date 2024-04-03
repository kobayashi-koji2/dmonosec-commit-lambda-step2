import time
from datetime import datetime
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from aws_lambda_powertools import Logger

logger = Logger()


# お知らせ未読フラグ取得
def get_announcement_flag(announcement_table, device_announcement_table, last_display_datetime, user_type, contract_id):

    # 現在日時取得
    now = datetime.now()
    now_unixtime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)

    # カウンター初期化
    count = 0

    # お知らせ管理未読数取得
    for announcement_type in ["system_maintenance", "important_announcement"]:
        count += announcement_table.query(
            IndexName="announcement_type_index",
            KeyConditionExpression=Key("announcement_type").eq(announcement_type)
            & Key("display_start_datetime").gte(last_display_datetime),
            FilterExpression=Attr("display_end_datetime").gte(now_unixtime)
        ).get("Count", 0)

    # デバイス関連お知らせ未読数取得
    if user_type in ["admin", "sub_admin"]:
        count += device_announcement_table.query(
            IndexName="contract_id_create_datetime_index",
            KeyConditionExpression=Key("contract_id").eq(contract_id)
            & Key("announcement_create_datetime").gte(last_display_datetime),
        ).get("Count", 0)

    # 未読数判定
    if count > 0:
        return 1
    else:
        return 0
