import json
import boto3
import db
import validate
import generate_detail
import ssm
import os
from jose import jwt
import logging
from decimal import Decimal
from botocore.exceptions import ClientError
dynamodb = boto3.resource('dynamodb')

SSM_KEY_TABLE_NAME = os.environ['SSM_KEY_TABLE_NAME']

parameter = None
logger = logging.getLogger()

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
            group_table = dynamodb.Table(parameter.get('GROUP_TABLE'))
        except KeyError as e:
            parameter = None
            body = {'code':'9999','message':e}
            return {
                'statusCode':500,
                'headers':res_headers,
                'body':json.dumps(body,ensure_ascii=False)
            }
        validate_result = validate.validate(event,user_table,account_table)
        if validate_result['code']!='0000':
            return {
                'statusCode': 200,
                'headers': res_headers,
                'body': json.dumps(validate_result, ensure_ascii=False)
            }
        device_id = event['pathParameters']['device_id']
        print(f'デバイスID:{device_id}')
        
        ##################
        # デバイス詳細取得
        ##################
        try:
            device_info = db.get_device_info(device_id,device_table).get('Items',{})[0]
            device_state = db.get_device_state(device_id,device_state_table).get('Item',{})
            group_info_list = []
            for item in device_info.get('device_data',{}).get('config',{}).get('group_list',{}):
                group_info_list.append(db.get_group_info(item,group_table).get('Item',{}))
            print(group_info_list)
            res_body = generate_detail.get_device_detail(device_info,device_state,group_info_list)
        except ClientError as e:
            print(e)
            body = {'code':'9999','message':'デバイス詳細の取得に失敗しました。'}
            return {
                'statusCode':500,
                'headers':res_headers,
                'body':json.dumps(body,ensure_ascii=False)
            }
        print(f'レスポンスボディ:{res_body}')
        return {
            'statusCode': 200,
            'headers': res_headers,
            'body': json.dumps(res_body, ensure_ascii=False, default=decimal_default_proc)
            #'body':res_body
        }
    except Exception as e:
        print(e)
        body = {'code':'9999','message':'予期しないエラーが発生しました。'}
        return {
            'statusCode':500,
            'headers':res_headers,
            'body':json.dumps(body,ensure_ascii=False)
        }
        
def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError