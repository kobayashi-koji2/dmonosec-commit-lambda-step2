import json
import boto3

from aws_lambda_powertools import Logger

# layer
import db
import convert

logger = Logger()


# パラメータチェック
def validate(event, tables):
    try:
        # 1.1 入力情報チェック
        decoded_idtoken = convert.decode_idtoken(event)
        # 1.2 トークンからユーザー情報取得
        user_id = decoded_idtoken["cognito:username"]
        # contract_id = decode_idtoken["contract_id"] #フェーズ2
    except Exception as e:
        logger.error(e)
        return {"code": "9999", "messege": "トークンの検証に失敗しました。"}
    # 1.3 ユーザー権限確認
    """
    account_info = db.get_account_info(user_id,tables["account_table"])
    logger.info(account_info)
    if account_info is None:
        return {
            "code":"9999",
            "messege":"アカウント情報が存在しません。"
        }
    account_id = account_info["account_id"]
    """
    # モノセコムユーザ管理テーブル取得
    user_info = db.get_user_info_by_user_id(user_id, tables["user_table"])
    if not user_info:
        return {"code": "9999", "messege": "ユーザ情報が存在しません。"}
    contract_info = db.get_contract_info(user_info["contract_id"], tables["contract_table"])
    if not contract_info:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}
    return {"code": "0000", "user_info": user_info}
