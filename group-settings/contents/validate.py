import json
import boto3
import db
from jose import jwt
import logging

logger = logging.getLogger()

# パラメータチェック
def validate(event,user_table):
    headers = event.get('headers',{})
    if not headers:
        return {
            'code':'9999',
            'messege':'リクエストパラメータが不正です。'
        }
    if 'Authorization' not in headers:
        return {
            'code':'9999',
            'messege':'リクエストパラメータが不正です。'
        }

    idtoken = event['headers']['Authorization']
    try:
        decoded_idtoken = decode_idtoken(idtoken)
        print('idtoken:', decoded_idtoken)
        user_id = decoded_idtoken['sub']
        #contract_id = decode_idtoke['contract_id'] 仕様未確定
    except  Exception as e:
        logger.error(e)
        return {
            'code':'9999',
            'messege':'トークンの検証に失敗しました。'
        }
    #ユーザの存在チェック
    user_info = db.get_user_info(user_id,user_table)
    if not 'Item' in user_info:
        return {
            'code':'9999',
            'messege':'ユーザ情報が存在しません。'
        }
    operation_auth = operation_auth_check(user_info)
    if not operation_auth:
        return {
            'code':'9999',
            'message':'グループの操作権限がありません。'
        }
    return {
        'code':'0000',
        'message':'',
        'decoded_idtoken':decoded_idtoken
    }

# Idトークンデコード
def decode_idtoken(idtoken):
    decoded_idtoken = jwt.get_unverified_claims(idtoken)
    return decoded_idtoken
    
# 操作権限チェック
def operation_auth_check(user_info):
    user_type, user_id = user_info['Item']['user_type'], user_info['Item']['user_id']
    #contract_id_list = []
    logger.debug(f'権限:{user_type}')
    if user_type == 'admin' or user_type == 'sub_admin':
        return True
    return False
