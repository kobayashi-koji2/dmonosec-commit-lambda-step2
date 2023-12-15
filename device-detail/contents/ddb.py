import json

import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')

# TODO テーブル名を環境変数に格納
user_table = dynamodb.Table('dmonosc-ddb-t-monosec-users')
device_table = dynamodb.Table('dmonosc-ddb-t-monosec-devices')
device_state_table = dynamodb.Table('dmonosc-ddb-t-monosec-device-state')
account_table = dynamodb.Table('dmonosc-ddb-m-office-accounts')
contract_table = dynamodb.Table('dmonosc-ddb-m-office-contracts')
pre_register_table = dynamodb.Table('dmonosc-ddb-t-monosec-pre-register-devices')
group_table = dynamodb.Table('dmonosc-ddb-t-monosec-groups')

#ユーザ情報取得
def get_user_info(user_id):
    user_info = user_table.get_item(
        Key={
            'user_id': user_id
        }
    )
    return user_info

#アカウント情報取得
def get_account_info(user_id):
    account_info = account_table.query(
        IndexName='user_id_index',
        KeyConditionExpression=Key('user_id').eq(user_id)
    )

    return account_info

#契約情報取得
def get_contract_info(contract_id):
    contract_info = contract_table.query(
        KeyConditionExpression=Key('contract_id').eq(contract_id)
    )
    return contract_info 

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

#現状態取得
def get_device_state(device_id):
    device_state = device_state_table.get_item(
        Key={
            'device_id': device_id
        }
    )
    return device_state
    
#グループ情報取得
def get_group_info(group_id):
    group_info = group_table.get_item(
        Key={
            'group_id': group_id
        }    
    )
    return group_info