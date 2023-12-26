import os
import json
import logging
import traceback
import boto3

# layer
import ssm
import validate
import db
import convert

logger = logging.getLogger()

# 環境変数
parameter = None
SSM_KEY_TABLE_NAME = os.environ["SSM_KEY_TABLE_NAME"]
AWS_DEFAULT_REGION = os.environ["AWS_DEFAULT_REGION"]
# 正常レスポンス内容
respons = {
    "statusCode": 200,
    "headers": {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
    },
    "body": "",
}
# AWSリソース定義
dynamodb = boto3.resource(
    "dynamodb",
    region_name=AWS_DEFAULT_REGION,
    endpoint_url=os.environ.get("endpoint_url")
)

def lambda_handler(event, context):
    try:
        ### 0. 環境変数の取得・DynamoDBの操作オブジェクト生成
        global parameter
        if parameter is None:
            ssm_params = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(ssm_params)
        else:
            print("parameter already exists. pass get_ssm_parameter")

        account_table = dynamodb.Table(parameter.get("ACCOUNT_TABLE"))
        user_table = dynamodb.Table(parameter["USER_TABLE"])

        ### 1. 入力情報チェック
        # 入力情報のバリデーションチェック
        val_result = validate.validate(event, user_table)
        if val_result["code"] != "0000":
            print("Error in validation check of input information.")
            respons["statusCode"] = 500
            respons["body"] = json.dumps(val_result, ensure_ascii=False)
            return respons
        # トークンからユーザー情報取得
        user_info = val_result["user_info"]["Item"]

        ### 2. デバイス表示順序更新
        # ユーザー情報取得
        # 1月まではいったん、ログインするユーザーIDとモノセコムユーザーIDは同じ認識で直接ユーザー管理より参照する形で実装
        # バリデーションチェックの処理の中でモノセコムユーザー管理より参照しているのでその値を使用

        # デバイス表示順序更新
        body = val_result["req_body"]
        af_device_list = body["device_list"]
        af_user_data = user_info["user_data"]
        af_user_data["config"]["device_order"] = af_device_list
        af_user_data = convert.to_dynamo_format(af_user_data)
        transact_items = [
            {
                "Update": {
                    "TableName": parameter["USER_TABLE"],
                    "Key": {
                        "user_id": {"S": user_info["user_id"]}
                    },
                    "UpdateExpression": "set #s = :s",
                    "ExpressionAttributeNames": {"#s" : "user_data"},
                    "ExpressionAttributeValues": {":s" : af_user_data}
                }
            }
        ]
        print(transact_items)
        result = db.execute_transact_write_item(transact_items)

        ### 3. メッセージ応答
        res_body = {
            "code": "0000",
            "message": "",
            "device_list": body["device_list"]
        }
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons
    except Exception as e:
        print(e)
        print(traceback.format_exc())
        res_body = {"code": "9999", "message": "予期しないエラーが発生しました。"}
        respons["statusCode"] = 500
        respons["body"] = json.dumps(res_body, ensure_ascii=False)
        return respons
