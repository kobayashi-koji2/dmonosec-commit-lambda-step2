from aws_lambda_powertools import Logger

# layer
import db
import convert

logger = Logger()


# パラメータチェック
def validate(event, user_table):
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "message": "パラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "パラメータが不正です。"}

    try:
        decoded_idtoken = convert.decode_idtoken(event)
        logger.debug("idtoken:", decoded_idtoken)
        user_id = decoded_idtoken["cognito:username"]
    except Exception:
        logger.error("トークンの検証に失敗", exc_info=True)
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    # ユーザの存在チェック
    user_res = db.get_user_info_by_user_id(user_id, user_table)
    if "Item" not in user_res:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    user = user_res["Item"]

    return {
        "code": "0000",
        "user_info": user,
        "decoded_idtoken": decoded_idtoken,
    }
