import json
import boto3
import logging

# layer
import db
import convert

logger = logging.getLogger()


# パラメータチェック
def validate(event, user_table):
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "message": "パラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "パラメータが不正です。"}

    idtoken = event["headers"]["Authorization"]
    try:
        decoded_idtoken = convert.decode_idtoken(idtoken)
        logger.debug("idtoken:", decoded_idtoken)
        user_id = decoded_idtoken["cognito:username"]
    except Exception as e:
        logger.error(e)
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    # ユーザの存在チェック
    user_info = db.get_user_info(user_id, user_table)
    if not "Item" in user_info:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}

    return {"code": "0000", "user_info": user_info, "decoded_idtoken": decoded_idtoken}
