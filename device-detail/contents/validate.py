import json
import boto3
import db
import ddb
import generate_detail
from jose import jwt
import logging
from decimal import Decimal

logger = logging.getLogger()

# Idトークンデコード
def decode_idtoken(idtoken):
    decoded_idtoken = jwt.get_unverified_claims(idtoken)
    return decoded_idtoken
    
# 操作権限チェック
def operation_auth_check(user_info,device_id,account_table):
    user_type, user_id = user_info['Item']['user_type'], user_info['Item']['user_id']
    contract_id_list = []
    logger.debug(f'権限:{user_type}')
    if user_type == 'admin':
        account_info = db.get_account_info(user_id,account_table).get("Items", [])
    elif user_type == 'sub_admin':
        parent_user_id = user_info['Item']['user_data']['config']['parent_user_id']
        account_info = db.get_account_info(parent_user_id,account_table).get("Items", [])
    elif user_type == 'worker' or user_type == 'referrer':
        if device_id in user_info['Item']['user_data']['management_devices']:
            return True
        return False
    for item in account_info:
        contract_id_list.append(item['contract_id'])
    device_id_list = ddb.get_device_id_all(contract_id_list)
    if device_id in device_id_list:
        return True
    
    return False

# パラメータチェック
def validate(event,user_table,account_table):
    headers = event.get('headers',{})
    pathParam = event.get('pathParameters',{})
    if not headers or not pathParam:
        return {
            'code':'9999',
            'messege':'パラメータが不正です。'
        }
    if 'Authorization' not in headers or 'device_id' not in pathParam:
        return {
            'code':'9999',
            'messege':'パラメータが不正です。'
        }

    idtoken = event['headers']['Authorization']
    device_id = event['pathParameters']['device_id']
    try:
        decoded_idtoken = decode_idtoken(idtoken)
        print('idtoken:', decoded_idtoken)
        user_id = decoded_idtoken['sub']
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
    operation_auth = operation_auth_check(user_info,device_id,account_table)
    if not operation_auth:
        return {
            'code':'9999',
            'message':'デバイスの操作権限がありません。'
        }
    
    return {
        'code':'0000',
        'message':''
    }