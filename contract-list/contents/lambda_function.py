import json
import traceback
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all

# layer
import convert
import auth

patch_all()
logger = Logger()


@auth.verify_login_user_list()
def lambda_handler(event, context, user_list):
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        ##################
        # 3 メッセージ応答
        ##################
        contract_id_list = [user["contract_id"] for user in user_list if "contract_id" in user]
        res_body = {
            "message": "",
            "contract_id_list": contract_id_list,
        }
        logger.info(f"レスポンスボディ:{res_body}")
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc),
        }
    except Exception as e:
        logger.info(e)
        logger.info(traceback.format_exc())
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }
