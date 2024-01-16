import re

from aws_lambda_powertools import Logger

# layer
import db

logger = Logger()


# パラメータチェック
def validate(event, user_info, account_table):
    auth_id = user_info["user_id"]
    # 1月まではいったん、IDトークンに含まれるusernameとモノセコムユーザーIDは同じ認識で直接ユーザー管理を参照するよう実装
    account_info = db.get_account_info(auth_id, account_table)
    if account_info is None:
        return {"message": "アカウント情報が存在しません。"}
    logger.info(f"account_info: {account_info}")
    logger.info(f"user_info: {user_info}")

    # 入力値チェック
    path_params = event.get("pathParameters", {})
    if not path_params:
        return {"message": "パスパラメータが不正です。"}
    # デバイスIDの必須チェック
    if "device_id" not in path_params:
        return {"message": "パスパラメータが不正です。"}
    # 接点出力端子番号の必須チェック
    if "do_no" not in path_params:
        return {"message": "パスパラメータが不正です。"}
    # デバイスIDの型チェック（半角英数字 & ハイフン）
    if not re.compile(r"^[a-zA-Z0-9\-]+$").match(path_params["device_id"]):
        return {"message": "不正なデバイスIDが指定されています。"}
    # 接点出力端子番号の型チェック（半角英数字）
    if not re.compile(r"^[a-zA-Z0-9]+$").match(path_params["do_no"]):
        return {"message": "不正な接点出力端子番号が指定されています。"}

    return {
        "path_params": path_params,
        "account_info": account_info,
    }
