from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from aws_lambda_powertools import Logger

logger = Logger()


# お知らせ管理情報取得
def get_announcement_list(announcement_table, announcement_type, now_unixtime):
    items = announcement_table.query(
        IndexName="announcement_type_index",
        KeyConditionExpression=Key("announcement_type").eq(announcement_type)
        & Key("display_start_datetime").lte(now_unixtime),
        FilterExpression=Attr("display_end_datetime").gte(now_unixtime)
    ).get("Items",[])

    announcement_list = []
    for item in items:
        announcement_list.append({
            "message": item.get("message"),
            "link_list": item.get("link_list", []),
        })

    return announcement_list


# デバイスお知らせ管理情報取得
def get_device_announcement_list(device_announcement_table, contract_id):
    device_announcement_list = device_announcement_table.query(
        IndexName="contract_id_create_datetime_index",
        KeyConditionExpression=Key("contract_id").eq(contract_id)
    ).get("Items",[])

    return device_announcement_list


# ユーザー情報（お知らせ画面最終表示日時）更新
def update_user_announcement_last_display_datetime(user_table, user_id, now_unixtime):
    option = {
        "Key": {
            "user_id": user_id
        },
        "UpdateExpression": "set #user_data.#config.#announcement_last_display_datetime = :now_unixtime",
        "ExpressionAttributeNames": {
            "#user_data": "user_data",
            "#config": "config",
            "#announcement_last_display_datetime": "announcement_last_display_datetime",
        },
        "ExpressionAttributeValues": {":now_unixtime": now_unixtime},
    }
    user_table.update_item(**option)
