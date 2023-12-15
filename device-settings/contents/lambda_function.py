import json
import boto3
import db
import ddb
import convert
import validate
import ssm
import os
from jose import jwt
import logging
from decimal import Decimal
from botocore.exceptions import ClientError

parameter = None
logger = logging.getLogger()
dynamodb = boto3.resource('dynamodb')

SSM_KEY_TABLE_NAME = os.environ['SSM_KEY_TABLE_NAME']

def lambda_handler(event, context):
    try:
        res_headers = {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
        #コールドスタートの場合パラメータストアから値を取得してグローバル変数にキャッシュ
        global parameter
        if not parameter:
            print('try ssm get parameter')
            response = ssm.get_ssm_params(SSM_KEY_TABLE_NAME)
            parameter = json.loads(response)
            print('tried ssm get parameter')
        else:
            print('passed ssm get parameter')
        # DynamoDB操作オブジェクト生成
        try:
            user_table = dynamodb.Table(parameter['USER_TABLE'])
            device_table = dynamodb.Table(parameter.get('DEVICE_TABLE'))
            device_state_table = dynamodb.Table(parameter.get('STATE_TABLE'))
            account_table = dynamodb.Table(parameter.get('ACCOUNT_TABLE'))
            contract_table = dynamodb.Table(parameter.get('CONTRACT_TABLE'))
            pre_register_table = dynamodb.Table(parameter.get('PRE_REGISTER_DEVICE_TABLE'))
            user_device_group_table = dynamodb.Table(parameter.get('USER_DEVICE_GROUP_TABLE'))
        except KeyError as e:
            parameter = None
            body = {'code':'9999','message':e}
            return {
                'statusCode':500,
                'headers':res_headers,
                'body':json.dumps(body,ensure_ascii=False)
            }
        #パラメータチェック
        validate_result = validate.validate(event,user_table,account_table,device_table)
        if validate_result['code']!='0000':
            return {
                'statusCode': 200,
                'headers': res_headers,
                'body': json.dumps(validate_result, ensure_ascii=False)
            }
        #デバイス設定更新
        body = json.loads(event['body'])
        device_id = event['pathParameters']['device_id']
        imei = body['device_imei']
        convert_param = convert.float_to_decimal(body)
        try:
            ddb.update_device_settings(device_id,imei,convert_param)
        except ClientError as e: 
            print(f'デバイス設定更新エラー e={e}')
            res_body = {
                'code':'9999',
                'message':'デバイス設定の更新に失敗しました。'
            }
            return {
                'statusCode':500,
                'headers':res_headers,
                'body': json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc)
            }
        else:
            # デバイス設定取得
            #device_info = db.get_device_info(device_id).get('Item',{}).get('device_data',{}).get('config',{})
            device_info = db.get_device_info(device_id,device_table)['Items'][0].get('device_data',{}).get('config',{})
        res_body = {
            'device_id':'',#DB設計変更
            'device_name':device_info['device_name'],
            'device_code':'',
            'device_iccid':'',
            'device_imei':'',
            'di_list':device_info.get('terminal_settings',{}).get('di_list',{}),
            'do_list':device_info.get('terminal_settings',{}).get('do_list',{}),
            'do_timer_list':device_info.get('terminal_settings',{}).get('do_timer_list',{}),
            'ai_list':device_info.get('terminal_settings',{}).get('ai_list',{})
        }
        return {
            'statusCode': 200,
            'headers': res_headers,
            'body': json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc)
            #body': res_body
        }
    except Exception as e:
        print(e)
        res_body = {
            'code':'9999',
            'message':'予期しないエラーが発生しました。'
        }
        return {
            'statusCode': 500,
            'headers': res_headers,
            'body': json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc)
        }
