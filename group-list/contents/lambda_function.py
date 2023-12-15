import json
import boto3
import db
import validate
import logging
from decimal import Decimal
from botocore.exceptions import ClientError

logger = logging.getLogger()

def lambda_handler(event, context):
    try:
        res_headers = {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
        #パラメータチェック
        validate_result = validate.validate(event)
        if validate_result['code']!='0000':
            return {
                'statusCode': 200,
                'headers': res_headers,
                'body': json.dumps(validate_result, ensure_ascii=False)
            }
        #contract_id = validate_result['decoded_idtoken']['contract_id'] 仕様未確定
        contract_id = 'na1234567'
        device_list, group_list = [],[]
        try:
            contract_info = db.get_contract_info(contract_id).get('Item',{})
            for group_id in contract_info.get('contract_data',{}).get('group_list',{}):
                group_info = db.get_group_info(group_id).get('Item',{})
                for device_id in group_info.get('group_data',{}).get('config',{}).get('device_list',{}):
                    device_info = db.get_device_info(device_id).get('Items',{})
                    if len(device_info)==0:
                        continue
                    device_list.append({
                        'device_id': device_id,
                        'device_name': device_info[0].get('device_data',{}).get('config',{}).get('device_name',{})
                    })
                group_list.append({
                    'group_id': group_id,
                    'group_name': group_info.get('group_data',{}).get('config',{}).get('group_name',{}),
                    'device_list': device_list
                })
                print(group_list)
        except ClientError as e:
            print(e)
            body = {'code':'9999','message':'グループ一覧の取得に失敗しました。'}
            return {
                'statusCode':500,
                'headers':res_headers,
                'body':json.dumps(body,ensure_ascii=False)
            }
        res_body = {
            'code':'0000',
            'message':'',
            'group_list': group_list
        }
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