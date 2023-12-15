import json
import os
import boto3
import logging
import validate
from botocore.exceptions import ClientError
dynamodb = boto3.resource('dynamodb')

# layer
import db
import ssm
import convert

# テスト
import db_dev
import convert_dev

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
            tables = {
                'user_table' : dynamodb.Table(parameter['USER_TABLE']),
                'device_table' : dynamodb.Table(parameter.get('DEVICE_TABLE')),
                'device_state_table' : dynamodb.Table(parameter.get('STATE_TABLE')),
                'account_table' : dynamodb.Table(parameter.get('ACCOUNT_TABLE')),
                'contract_table' : dynamodb.Table(parameter.get('CONTRACT_TABLE')),
                'pre_register_table' : dynamodb.Table(parameter.get('PRE_REGISTER_DEVICE_TABLE')),
                'device_relation_table' : dynamodb.Table(parameter.get('DEVICE_RELATION_TABLE'))
            }
        except KeyError as e:
            parameter = None
            body = {'code':'9999','message':e}
            return {
                'statusCode':500,
                'headers':res_headers,
                'body':json.dumps(body,ensure_ascii=False)
            }
        ##################
        # 入力情報チェック
        ##################
        validate_result = validate.validate(event,tables)
        if validate_result['code']!='0000':
            return {
                'statusCode': 200,
                'headers': res_headers,
                'body': json.dumps(validate_result, ensure_ascii=False)
            }
        
        user_info = validate_result['user_info']['Items'][0]
        user_id = user_info['user_id']
        user_type = user_info['user_type']
        contract_id = validate_result['contract_id']
        #print(user_id,user_type,contract_id)
        print(f'ユーザ情報:{user_info}')
        
        ##################
        # デバイスID一覧取得
        ##################
        #device_list = get_device_list(device_order_list, device_table, device_state_table)
        #user_type = 'worker'
        print(f'権限:{user_type}')
        device_id_list =[]
        if user_type == 'admin' or user_type == 'sub_admin':
            #契約管理テーブル取得
            contract_info = db_dev.get_contract_info(contract_id,tables['contract_table'])
            if 'Item' not in contract_info:
                res_body = {'code':'9999','message':'契約情報が存在しません。'}
                return {
                    'statusCode':500,
                    'headers':res_headers,
                    'body':json.dumps(res_body,ensure_ascii=False)
                }
            device_id_list = contract_info.get('Item',{}).get('contract_data',{}).get('device_list',[])
        elif user_type == 'worker' or user_type == 'referrer':
            #デバイス関連テーブル取得
            device_relation = db_dev.get_device_relation(f'u-{user_id}',tables['device_relation_table'])
            print(device_relation)
            for item1 in device_relation:
                item1 = item1['key2']
                #ユーザに紐づくデバイスIDを取得
                if item1.startswith('d-'):
                    device_id_list.append(item1)
                #グループIDをキーにデバイスIDを取得
                elif item1.startswith('g-'):
                    device_group_relation = db_dev.get_device_relation(item1, tables['device_relation_table'], sk_prefix='d-')
                    for item2 in device_group_relation:
                        device_id_list.append(item2['key2'])
            #重複削除
            device_id_list = set(device_id_list)
        else:
            res_body = {
                'code':'9999',
                'messege':'不正なユーザです。'
            }
        print(f'デバイスID:{device_id_list}')
        
        ##################
        # デバイス順序更新
        ##################
        #順序取得
        device_order = user_info.get('user_data',{}).get('config',{}).get('device_order',[])
        print(f'デバイス順序:{device_order}')
        #順序比較
        device_order_update = device_order_comparison(device_order,device_id_list)
        #順序更新
        if device_order_update:
            print('try device order update')
            print(f'最新のデバイス順序:{device_order_update}')
            res = db_dev.update_device_order(device_order_update,user_id,tables['user_table'])
            print('tried device order update')
        else:
            print('passed device order update')
        
        ##################
        # グループ名一覧取得
        ##################
        '''
        try:
            print('デバイス一覧取得')
        except ClientError as e:
            print(e)
            body = {'code':'9999','message':'デバイス一覧の取得に失敗しました。'}
            return {
                'statusCode':500,
                'headers':res_headers,
                'body':json.dumps(body,ensure_ascii=False)
            }
        '''
        res_body = {'code':'0000','message':'成功'}
        print(f'レスポンス:{res_body}')
        return {
            'statusCode': 200,
            'headers': res_headers,
            'body':  json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc)
        }
    except Exception as e:
        print(e)
        body = {'code':'9999','message':'予期しないエラーが発生しました。'}
        return {
            'statusCode':500,
            'headers': res_headers,
            'body':json.dumps(body,ensure_ascii=False)
        }

#デバイス一覧取得
def get_device_list(device_list, device_table, device_state_table):
    device_state_list, not_device_info_list, not_state_info_list = [],[],[]
    order = 0
    for item in device_list:
        order+=1
        device_info = db.get_device_info(item, device_table)
        #デバイス現状態取得
        device_state = db.get_device_state(item, device_state_table)
        if len(device_info['Items'])==0:
            not_device_info_list.append(item)
            continue
        elif 'Item' not in device_state:
            not_state_info_list.append(item)
            #レスポンス生成(現状態情報なし)
            device_state_list.append({
                'device_id':item,
                'device_name':device_info['Items'][0]['device_data']['config']['device_name'],
                'device_imei':device_info['Items'][0]['imei'],
                'group_name_list':[],
                'device_order':order,
                'signal_status': '',
                'battery_near_status': '',
                'device_abnormality': '',
                'parameter_abnormality': '',
                'fw_update_abnormality': '',
                'device_unhealthy':'',
                'di_unhealthy':''
             })
            continue
        #レスポンス生成(現状態情報あり)
        device_state_list.append({
                'device_id':item,
                'device_name':device_info['Items'][0]['device_data']['config']['device_name'],
                'device_imei':device_info['Items'][0]['imei'],
                'group_name_list':[],
                'device_order':order,
                'signal_status': device_state['Item']['signal_status'],
                'battery_near_status': device_state['Item']['battery_near_status'],
                'device_abnormality': device_state['Item']['device_abnormality'],
                'parameter_abnormality': device_state['Item']['parameter_abnormality'],
                'fw_update_abnormality': device_state['Item']['fw_abnormality'],
                'device_unhealthy':'',
                'di_unhealthy':''
        })

    print('情報が存在しないデバイス:',not_device_info_list)
    print('現状態が存在しないデバイス:',not_state_info_list)
    return device_state_list

#順序比較
def device_order_comparison(device_order,device_id_list):
    if set(device_order) == set(device_id_list):
        return False
    if set(device_order) - set(device_id_list):
        diff1 = list(set(device_order) - set(device_id_list))
        print(f'diff1:{diff1}')
        device_order = [item for item in device_order if item not in diff1]
    if set(device_id_list) - set(device_order):
        diff2 = list(set(device_id_list) - set(device_order))
        print(f'diff2:{diff2}')
        device_order = device_order + diff2
    print(device_order)
    return device_order