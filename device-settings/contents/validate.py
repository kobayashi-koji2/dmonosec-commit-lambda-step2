import json
import boto3
import ddb
import logging
import re

# layer
import db
import convert

logger = logging.getLogger()

# パラメータチェック
def validate(event, tables):
    headers = event.get('headers',{})
    pathParam = event.get('pathParameters',{})
    body = event.get('body',{})
    if not headers or not pathParam or not body:
        return {
            'code':'9999',
            'messege':'リクエストパラメータが不正です。'
        }
    if 'Authorization' not in headers or 'device_id' not in pathParam or 'device_imei' not in body:
        return {
            'code':'9999',
            'messege':'リクエストパラメータが不正です。'
        }

    device_id = event['pathParameters']['device_id']
    body = json.loads(body)

    try:
        decoded_idtoken = convert.decode_idtoken(event)
        print('idtoken:', decoded_idtoken)
        user_id = decoded_idtoken["cognito:username"]
    except  Exception as e:
        logger.error(e)
        return {
            'code':'9999',
            'messege':'トークンの検証に失敗しました。'
        }

    #1.3 ユーザー権限確認
    #モノセコムユーザ管理テーブル取得
    user_info = db.get_user_info_by_user_id(user_id,tables['user_table'])
    if "Item" not in user_info:
        return {
            'code':'9999',
            'messege':'ユーザ情報が存在しません。'
        }
    contract_info = db.get_contract_info(user_info['Item']['contract_id'], tables['contract_table'])
    if "Item" not in contract_info:
        return {"code": "9999", "messege": "アカウント情報が存在しません。"}

    ##################
    # 2 デバイス操作権限チェック
    ##################
    device_info = ddb.get_device_info_by_id_imei(
        device_id,
        body['device_imei'],
        tables['device_table']
    )
    if 'Item' not in device_info:
        return {
            'code':'9999',
            'message':'デバイス情報が存在しません。'
        }
    
    operation_auth = operation_auth_check(user_info, contract_info, device_id, tables)
    if not operation_auth:
        return {
            'code':'9999',
            'message':'不正なデバイスIDが指定されています。'
        }
    # 端子設定チェック
    terminal = terminal_check(body,device_id, device_info['Item']['device_type'], tables)
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
        'message':'',
        'device_id': device_id,
        'body':body
        
    }

# 操作権限チェック
def operation_auth_check(user_info, contract_info, device_id, tables):
    user_type, user_id = user_info['Item']['user_type'], user_info['Item']['user_id']
    contract_id_list = []
    # 2.1 デバイスID一覧取得
    accunt_devices = contract_info['Item']['contract_data']['device_list']
    print(f'ユーザID:{user_id}')
    print(f'権限:{user_type}')
    if device_id not in accunt_devices:
        return False

    if user_type == 'referrer':
        return False
    if user_type == 'worker':
        # 3.1 ユーザに紐づくデバイスID取得
        user_devices = []
        device_relation = db.get_device_relation(f'u-{user_id}',tables['device_relation_table'])
        for item1 in device_relation:
            item1 = item1['key2']
            #ユーザに紐づくデバイスIDを取得
            if item1.startswith('d-'):
                user_devices.append(re.sub('^d-', '', item1))
            #グループIDをキーにデバイスIDを取得
            elif item1.startswith('g-'):
                device_group_relation = db.get_device_relation(item1, tables['device_relation_table'], sk_prefix='d-')
                for item2 in device_group_relation:
                    user_devices.append(re.sub('^d-', '', item2['key2']))

        if device_id not in set(user_devices):
            return False
    return True

# 端子設定チェック
def terminal_check(body,device_id,device_type,tables):
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
#画面一覧に記載のされている入力制限のみチェック
def input_check(param):
    out_range_list, null_list, invalid_format_list, invalid_data_type = [],[],[],[]
    
    #文字数の制限
    #デバイス名、接点名、ON-OFF名は未登録の場合、WEB側で初期値を表示する仕様のため空文字を許容する
    str_value_limits = {
        'device_name':{0, 30},
        'di_name':{0, 30},
        'di_on_name':{0, 10},
        'di_on_icon':{1, 30},
        'di_off_name':{0, 10},
        'di_off_icon':{1, 30},
        'do_name':{0, 30},
        'do_on_name':{0, 10},
        'do_on_icon':{1, 30},
        'do_off_name':{0, 10},
        'do_off_icon':{1, 30},
    }
    
    #桁数の制限
    int_float_value_limits = {
        'do_specified_time':{0.4, 6553.5},
        'do_onoff_control':{0, 1}
    }
    
    #アイコン(TBD) コード一覧参照
    icon_list = [
        'stete_icon1',
        'state_icon2',
        'state_icon3',
        'state_icon4',
        'state_icon5',
        'state_icon6',
        'state_icon7',
        'state_icon8'
    ]
    
    str_format = {
        'di_on_icon': icon_list,
        'di_off_icon': icon_list,
        'do_on_icon': icon_list,
        'do_off_icon' : icon_list,
        'do_control': ['open','close','toggle'],
    }

    #dict型の全要素を探索して入力値をチェック
    def check_dict_value(param):
        if isinstance(param, dict):
            for key, value in param.items():
                #文字列
                if isinstance(value, str):
                    #データ型
                    if key in int_float_value_limits:
                        print(f'Key:{key}  value:{value} - reason:データ型が不正です。')
                        invalid_data_type_list.append(key)
                    #文字数
                    elif key in str_value_limits:
                        min_length,max_length = str_value_limits[key]
                        string_length = len(value)
                        if not min_length <= string_length <= max_length:
                            print(f'Key:{key}  value:{value} - reason:文字数の制限を超えています。')
                            out_range_list.append(key)
                    #文字列フォーマット
                    if key in str_format:
                        valid_strings = str_format[key]
                        if value not in valid_strings:
                            print(f'Key:{key}  value:{value} - reason:文字列の形式が不正です。')
                            invalid_format_list.append(key)
                #数値
                elif isinstance(value, (int, float)):
                    #データ型
                    if key in str_value_limits:
                        print(f'Key:{key}  value:{value} - reason:データ型が不正です。')
                        invalid_data_type_list.append(key)
                    #桁数
                    elif key in int_float_value_limits:
                        min_value,max_value = int_float_value_limits[key]
                        if not float(min_value) <= float(value) <= float(max_value):
                            print(f'Key:{key}  value:{value} - reason:桁数の制限を超えています。')
                            out_range_list.append(key)
                else:
                    check_dict_value(value)
        elif isinstance(param, list):
            for item in param:
                check_dict_value(item)
        return out_range_list

    out_range_list = check_dict_value(param)
    if len(out_range_list)==0 and len(invalid_format_list)==0:
        return True
    return False
