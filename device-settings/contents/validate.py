import json
import boto3
import db
import ddb
from jose import jwt
import logging

logger = logging.getLogger()

# パラメータチェック
def validate(event,user_table,account_table,device_table):
    headers = event.get('headers',{})
    pathParam = event.get('pathParameters',{})
    #body = event.get('body',{})
    body = json.loads(event['body'])
    if not headers or not pathParam or not body:
        return {
            'code':'9999',
            'messege':'リクエストパラメータが不正です。'
        }
    if 'Authorization' not in headers or 'device_id' not in pathParam:
        return {
            'code':'9999',
            'messege':'リクエストパラメータが不正です。'
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
        
    terminal = terminal_check(body,device_id,device_table)
    if not terminal:
        return {
            'code':'9999',
            'message':'デバイス種別と端子設定が一致しません。'
        }
    
    input = input_check(body)
    if not input:
        return {
            'code':'9999',
            'message':'入力パラメータが不正です。'
        }
    return {
        'code':'0000',
        'message':''
    }

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
    
# 端子設定チェック
def terminal_check(body,device_id,device_table):
    #device_type = db.get_device_info(device_id).get('Item',{}).get('device_type',{})
    device_type = db.get_device_info(device_id,device_table)['Items'][0]['device_type']
    di, do, ai = len(body.get('di_list',{})), len(body.get('do_list',{})), len(body.get('ai_list',{}))
    di_no_list, do_no_list, ai_no_list = [],[],[]
    #デバイス種別と端子数
    if (device_type == 1 and di == 1 and do == 0 and ai == 0)\
    or (device_type == 2 and di == 8 and do == 2 and ai == 0)\
    or (device_type == 3 and di == 8 and do == 2 and ai == 2):
        #端子番号
        for item in body.get('di_list',{}):
            di_no_list.append(item.get('di_no'))
        for item in body.get('do_list',{}):
            do_no_list.append(item.get('do_no'))
        for item in body.get('ai_list',{}):
            ai_no_list.append(item.get('ai_no'))
        #端子番号の範囲
        if all(1 <= num <= di for num in di_no_list)\
        and all(1<= num <= do for num in do_no_list)\
        and all(1<= num <= ai for num in ai_no_list):
            #端子番号の重複
            if len(set(di_no_list)) == len(di_no_list)\
            and len(set(do_no_list)) == len(do_no_list)\
            and len(set(ai_no_list)) == len(ai_no_list):
                return True
    return False

#入力チェック
def input_check(param):
    out_range_list, null_list = [],[]
    value_limits = {
        'device_name':{0, 30},
        'di_name':{0, 30},
        'di_on_name':{0, 10},
        'di_on_icon':{1, 30},
        'di_off_name':{0, 10},
        'di_off_icon':{1, 30},
        'do_name':{0, 30},
        'do_on_name':{0, 10},
        'do_off_name':{0, 10},
        'ai_name':{0, 30},
        'do_specified_time':{0.4, 6553.5},
        'do_onoff_control':{0, 1}
    }

    #文字数、数値範囲
    def check_dict_value_limits(param):
        if isinstance(param, dict):
            for key, value in param.items():
                if key in value_limits and isinstance(value, str):
                    min_length,max_length = value_limits[key]
                    string_length = len(value)
                    if not min_length <= string_length <= max_length:
                        print(f'Key:{key}  value:{value} - reason:文字数制限の範囲外です。')
                        out_range_list.append(key)
                elif key in value_limits and isinstance(value, (int, float)):
                    min_value,max_value = value_limits[key]
                    if not float(min_value) <= float(value) <= float(max_value):
                        print(f'Key:{key}  value:{value} - reason:正常な数値の範囲外です。')
                        out_range_list.append(key)
                else:
                    check_dict_value_limits(value)
        elif isinstance(param, list):
            for item in param:
                check_dict_value_limits(item)
        return out_range_list

    out_range_list = check_dict_value_limits(param)
    if len(out_range_list)==0:
        return True
    return False
