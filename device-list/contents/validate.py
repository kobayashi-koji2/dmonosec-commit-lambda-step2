import json
import boto3
import logging

# layer
import db
import convert

import db_dev
import convert_dev

logger = logging.getLogger()

# パラメータチェック
def validate(event,tables):
    '''
    headers = event.get('headers',{})
    if not headers:
        return {
            'code':'9999',
            'message':'パラメータが不正です。'
        }
    if 'Authorization' not in headers:
        return {
            'code':'9999',
            'messege':'パラメータが不正です。'
        }   
    idtoken = event['headers']['Authorization']
    '''
    #Cognito IdTokenチェック
    try:
        decoded_idtoken = convert_dev.decode_idtoken(event)
        #print('idtoken:', decoded_idtoken)
        auth_id = decoded_idtoken['cognito:username']
        #contract_id = decode_idtoken['contract_id'] -----idtokenのカスタム機能実装後にコメントアウト解除-----
        contract_id = 'hogehogefugafuga'
        print(f'認証ID:{auth_id}')
        print(f'契約ID:{contract_id}')
    except  Exception as e:
        logger.error(e)
        return {
            'code':'9999',
            'messege':'トークンの検証に失敗しました。'
        }
    #アカウント情報取得
    account_info = db_dev.get_account_info(auth_id,tables['account_table'])
    print(account_info)
    if len(account_info['Items']) == 0:
        return {
            'code':'9999',
            'messege':'ユーザに紐づくアカウント情報が存在しません。'
        }
    account_id = account_info['Items'][0]['account_id']
    #モノセコムユーザ管理テーブル取得
    user_info = db_dev.get_user_info(account_id,contract_id,tables['user_table'])
    if len(user_info['Items']) == 0:
        return {
            'code':'9999',
            'messege':'ユーザ情報が存在しません。'
        }
    print(user_info)
    return {
        'code':'0000',
        'user_info':user_info,
        'auth_id': auth_id,
        'contract_id':contract_id
    }