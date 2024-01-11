import os
import json

import boto3

ssm = boto3.client(
    "ssm", region_name="ap-northeast-1", endpoint_url=os.environ.get("endpoint_url")
)


##################################
# パラメータストアから値を取得
##################################
def get_ssm_params(key):
    result = {}
    if isinstance(key, tuple) and len(key) > 1:
        response = ssm.get_parameters(
            Names=key,
            WithDecryption=True,
        )

        for p in response["Parameters"]:
            result[p["Name"]] = p["Value"]
    elif isinstance(key, str):
        response = ssm.get_parameter(Name=key, WithDecryption=True)
        result = response["Parameter"]["Value"]
    return result


##################################
# 初期処理
# SSMからテーブル名を取得して、モジュールとしてインポート可能にする
##################################
def _init():
    ssm_key_table_name = os.environ.get("SSM_KEY_TABLE_NAME")
    if ssm_key_table_name:
        response = get_ssm_params(ssm_key_table_name)
        global table_names
        table_names = json.loads(response)


_init()
