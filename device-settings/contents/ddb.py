import json

import boto3
from boto3.dynamodb.conditions import Key
import decimal


dynamodb = boto3.resource('dynamodb')

# TODO テーブル名を環境変数に格納
user_table = dynamodb.Table('dmonosc-ddb-t-monosec-users')
device_table = dynamodb.Table('dmonosc-ddb-t-monosec-devices')
device_state_table = dynamodb.Table('dmonosc-ddb-t-monosec-device-state')
account_table = dynamodb.Table('dmonosc-ddb-m-office-accounts')
contract_table = dynamodb.Table('dmonosc-ddb-m-office-contracts')
pre_register_table = dynamodb.Table('dmonosc-ddb-t-monosec-pre-register-devices')

#ユーザ情報取得
def get_user_info(user_id):
    user_table_res = user_table.get_item(
        Key={
            'user_id': user_id
        }
    )
    return user_table_res

#アカウント情報取得
def get_account_info(user_id):
    account_table_res = account_table.query(
        IndexName='user_id_index',
        KeyConditionExpression=Key('user_id').eq(user_id)
    )

    return account_table_res

#契約情報取得
def get_contract_info(contract_id):
    contract_table_res = contract_table.query(
        KeyConditionExpression=Key('contract_id').eq(contract_id)
    )
    return contract_table_res 

#アカウントに紐づくデバイスID取得
def get_device_id_all(contract_id_list):
    device_id_list = []
    for item in contract_id_list:
        contract_info = get_contract_info(item)
        device_id_list+=contract_info['Items'][0]['contract_data']['device_list']

    return device_id_list


#デバイス情報取得
def get_device_info(device_id):
    device_info = device_table.query(
        IndexName='contract_state_index',
        KeyConditionExpression=Key('device_id').eq(device_id) & Key('contract_state').eq(1)
    )
    return device_info

#デバイス設定更新
def update_device_settings(device_id,imei,device_settings):
    map_attribute_name = 'device_data'
    sub_attribute_name1 = 'config'
    sub_attribute_name2 = 'terminal_settings'
    device_name, di_new_val, do_new_val, ai_new_val = device_settings.get('device_name',{}), device_settings.get('di_list',{}), device_settings.get('do_list',{}), device_settings.get('ai_list',{})
    di_key, do_key, ai_key, device_name_key = 'di_list', 'do_list', 'ai_list', 'device_name'
    update_expression = f"SET #map.#sub1.#device_name_key = :device_name,\
                        #map.#sub1.#sub2.#di_key = :di_new_val,\
                        #map.#sub1.#sub2.#do_key = :do_new_val,\
                        #map.#sub1.#sub2.#ai_key = :ai_new_val"
    expression_attribute_values = {
        ':di_new_val': di_new_val,
        ':do_new_val': do_new_val,
        ':ai_new_val': ai_new_val,
        ':device_name': device_name
    }
    expression_attribute_name = {
        '#map': map_attribute_name,
        '#sub1': sub_attribute_name1,
        '#sub2': sub_attribute_name2,
        '#di_key': di_key,
        '#do_key': do_key,
        '#ai_key': ai_key,
        '#device_name_key': device_name_key
    }
    device_table.update_item(
        Key={
            'device_id': device_id,
            'imei':imei
        },
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name
    )
    return

#現状態取得
'''
def get_device_state(device_id):
    device_state_table_res = device_state_table.get_item(
        Key={
            'device_id': device_id
        }
    )
    return device_state_table_res
    
#デバイスが所属するグループ一覧取得
def get_device_group():
    return
'''