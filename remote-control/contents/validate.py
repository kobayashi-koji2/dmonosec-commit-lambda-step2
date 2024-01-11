import re

from aws_lambda_powertools import Logger

# layer
import db
import convert

logger = Logger()


# パラメータチェック
def validate(event, account_table, user_table):
    headers = event.get("headers", {})
    if not headers:
        return {"code": "9999", "message": "パラメータが不正です。"}
    if "Authorization" not in headers:
        return {"code": "9999", "messege": "パラメータが不正です。"}
    decoded_idtoken = convert.decode_idtoken(event)

    # リクエストユーザの存在チェック
    auth_id = decoded_idtoken["cognito:username"]
    # 1月まではいったん、IDトークンに含まれるusernameとモノセコムユーザーIDは同じ認識で直接ユーザー管理を参照するよう実装
    account_info = db.get_account_info(auth_id, account_table)
    if not "Items" in account_info:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}
    account_info = account_info["Items"][0]
    logger.info("account_info", end=": ")
    logger.info(account_info)

    user_info = db.get_user_info_by_user_id(auth_id, user_table)
    if not "Item" in user_info:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    user_info = user_info["Item"]
    logger.info("user_info", end=": ")
    logger.info(user_info)

    # 入力値チェック
    path_params = event.get("pathParameters", {})
    if not path_params:
        return {"code": "9999", "message": "パスパラメータが不正です。"}
    # デバイスIDの必須チェック
    if "device_id" not in path_params:
        return {"code": "9999", "messege": "パスパラメータが不正です。"}
    # 接点出力端子番号の必須チェック
    if "do_no" not in path_params:
        return {"code": "9999", "messege": "パスパラメータが不正です。"}
    # デバイスIDの型チェック（半角英数字 & ハイフン）
    if not re.compile(r"^[a-zA-Z0-9\-]+$").match(path_params["device_id"]):
        return {"code": "9999", "messege": "不正なデバイスIDが指定されています。"}
    # 接点出力端子番号の型チェック（半角英数字）
    if not re.compile(r"^[a-zA-Z0-9]+$").match(path_params["do_no"]):
        return {"code": "9999", "messege": "不正な接点出力端子番号が指定されています。"}

    return {
        "code": "0000",
        "decoded_idtoken": decoded_idtoken,
        "path_params": path_params,
        "account_info": account_info,
        "user_info": user_info,
    }
