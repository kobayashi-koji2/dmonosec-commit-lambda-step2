from aws_lambda_powertools import Logger

# layer
import db
import convert

logger = Logger()


# パラメータチェック
def validate(event, user_table):
    # リクエストユーザの存在チェック
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "message": "パラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "パラメータが不正です。"}

    decoded_idtoken = convert.decode_idtoken(event)
    if decoded_idtoken == False:
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    user_id = decoded_idtoken["cognito:username"]
    user_info = db.get_user_info_by_user_id(user_id, user_table)
    if not "Item" in user_info:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}

    return {
        "code": "0000",
        "user_info": user_info,
        "decoded_idtoken": decoded_idtoken,
    }
