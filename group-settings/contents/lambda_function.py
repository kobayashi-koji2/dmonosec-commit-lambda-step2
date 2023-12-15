import json
import boto3
import db
import validate
import group
import ssm
import os
import logging
import convert
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
            group_table = dynamodb.Table(parameter.get('GROUP_TABLE'))
            account_table = dynamodb.Table(parameter.get('ACCOUNT_TABLE'))
            contract_table = dynamodb.Table(parameter.get('CONTRACT_TABLE'))
            pre_register_table = dynamodb.Table(parameter.get('PRE_REGISTER_DEVICE_TABLE'))
        except KeyError as e:
            parameter = None
            body = {'code':'9999','message':e}
            return {
                'statusCode':500,
                'headers':res_headers,
                'body':json.dumps(body,ensure_ascii=False)
            }
        #パラメータチェック
        validate_result = validate.validate(event,user_table)
        if validate_result['code']!='0000':
            return {
                'statusCode': 200,
                'headers': res_headers,
                'body': json.dumps(validate_result, ensure_ascii=False)
            }
        group_info = json.loads(event['body']) #APIGW呼び出し
        #group_info = event['body'] Lambdaテスト呼び出し
        
        #グループ新規登録
        if event['httpMethod'] == 'POST':
            result = group.create_group_info(group_info,contract_table,device_table)
        #グループ更新
        elif event['httpMethod'] == 'PUT':
            group_id = event['pathParameters']['group_id']
            result = group.update_group_info(group_info,group_id,group_table,device_table)
        
        if result[0]:
            group_info = db.get_group_info(result[1],group_table).get('Item',{})
            device_list = []
            for item in group_info.get('group_data',{}).get('config',{}).get('device_list',{}):
                device_info = db.get_device_info(item,device_table).get('Items',{})
                if len(device_info)==0:
                    continue
                device_name = device_info[0].get('device_data',{}).get('config',{}).get('device_name',{})
                device_list.append({
                    'device_id': item,
                    'device_name': device_name
                })
            res_body = {
                'code':'0000',
                'message':'',
                'group_id': group_info['group_id'],
                'group_name': group_info.get('group_data',{}).get('config',{}).get('group_name',{}),
                'device_list': device_list
            }
        else:
            res_body = {
                'code':'9999',
                'message':'グループの登録・更新に失敗しました。'
            }
            
        return {
            'statusCode': 200,
            'headers': res_headers,
            'body': json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc)
            #'body': res_body
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