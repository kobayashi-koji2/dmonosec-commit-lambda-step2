import boto3
from boto3.dynamodb.conditions import Key


dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb', region_name='ap-northeast-1')

#ユーザ情報取得
def get_user_info(user_id,user_table):
    user_table = user_table.get_item(
        Key={
            'user_id': user_id
        }
    )
    return user_table

#アカウント情報取得
def get_account_info(user_id,account_table):
    account_table = account_table.query(
        IndexName='user_id_index',
        KeyConditionExpression=Key('user_id').eq(user_id)
    )

    return account_table

#契約情報取得
def get_contract_info(contract_id,contract_table):
    contract_info = contract_table.get_item(
        Key={
            'contract_id':contract_id
        }
    )
    return contract_info

#アカウントに紐づくデバイスID取得
def get_device_id_all(contract_id_list,contract_table):
    device_id_list = []
    for item in contract_id_list:
        contract_info = get_contract_info(item,contract_table)
        device_id_list+=contract_info['Items'][0]['contract_data']['device_list']

    return device_id_list


#デバイス情報取得
def get_device_info(device_id,device_table):
    device_info = device_table.query(
        IndexName='contract_state_index',
        KeyConditionExpression=Key('device_id').eq(device_id) & Key('contract_state').eq(1)
    )
    return device_info
    
#グループ情報取得
def get_group_info(group_id,group_table):
    group_info = group_table.get_item(
        Key={
            'group_id': group_id
        }    
    )
    return group_info

#トランザクション(書き込み)
def execute_transact_write_item(transact_items):
    try:
        client.transact_write_items(
            TransactItems=transact_items
        )
        return True
    except Exception as e:
        print(e)
        return False
