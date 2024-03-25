from aws_lambda_powertools import Logger
import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr


logger = Logger()
dynamodb = boto3.resource("dynamodb")


# デバイス情報取得(契約状態:使用不可以外)
def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    )
    return response


# タイマー設定削除
def delete_timer_settings(device_id, imei, device_settings, table):
    map_attribute_name = "device_data"
    sub_attribute_name1 = "config"
    sub_attribute_name2 = "terminal_settings"

    do_new_val = device_settings.get("do_list", {})
    logger.info(f"delete{do_new_val}")
    for do in do_new_val:
        logger.info(f"delete{do_new_val}")
        do_timer_list = do.get("do_timer_list", [])
        for do_timer in do_timer_list:
            logger.info(f"delete{do_new_val}")
            request_do_timer_id = do_timer.get("do_timer_id", "")
            # デバイステーブルからデバイス情報取得
            device_info = get_device_info(device_id, table).get("Items", {})
            device_info = device_info[0]
            do_list = device_info["device_data"]["config"]["terminal_settings"]["do_list"]
            for do in do_list:
                logger.info(f"delete{do_list}")
                do_timer_list = do.get("do_timer_list", [])
                # リクエストの接点出力_端子番号と一致すればタイマー設定削除
                for do_timer in do_timer_list:
                    do_timer_id = do_timer.get("do_timer_id", "")
                    logger.info(f"delete{do_timer_id}, {request_do_timer_id}")
                    if request_do_timer_id == do_timer_id:
                        do_timer_list.clear()

            do_key = "do_list"
            update_expression = "SET #map.#sub1.#sub2.#do_key = :do_list"
            expression_attribute_values = {":do_list": do_list}
            expression_attribute_name = {
                "#map": map_attribute_name,
                "#sub1": sub_attribute_name1,
                "#sub2": sub_attribute_name2,
                "#do_key": do_key,
            }
            table.update_item(
                Key={"device_id": device_id, "imei": imei},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values,
                ExpressionAttributeNames=expression_attribute_name,
            )
            return True
