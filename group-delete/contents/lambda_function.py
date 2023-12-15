import json
import boto3
import ddb
import validate
import ssm
import logging
import os
import convert

dynamodb = boto3.resource('dynamodb')
parameter = None

SSM_KEY_TABLE_NAME = os.environ['SSM_KEY_TABLE_NAME']

def lambda_handler(event, context):

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
    #パラメータチェック
    validate_result = validate.validate(event,user_table,account_table,device_table)
    if validate_result['code']!='0000':
        return {
            'statusCode': 200,
            'headers': res_headers,
            'body': json.dumps(validate_result, ensure_ascii=False)
        }
    decoded_idtoken = validate_result['decoded_idtoken']
    user_id = decoded_idtoken['cognito:username']
    group_id = event['pathParameters']['group_id']
    device_list = ddb.get_group_info(group_id, group_table).get('Item',{}).get('group_data',{}).get('config',{}).get('device_list',[])
    group_info = ddb.get_group_info(group_id, group_table).get('Item',{})
    group_list = group_info.get('group_data',{}).get('config',{}).get('group_list',{})
    user_list = group_info.get('group_data',{}).get('config',{}).get('user_list',{})
    #contract_id = decoded_idtoke['contrack_id'] #未実装
    contract_id = 'na1234567'
    #トランザクション書き込み用オブジェクト
    transact_items,delete_group,update_device,update_user,update_contract = [],[],[],[],[]
    #テーブル更新用キー
    contract_data_attr = 'contract_data'
    device_data_attr = 'device_data'
    user_data_attr = 'user_data'
    config_attr = 'config'
    device_list_attr = 'device_list'
    group_list_attr = 'group_list'

    #################################################
    # グループ管理テーブル更新用オブジェクト作成
    #################################################
    delete_group = {
                'Delete':{
                    'TableName': parameter.get('GROUP_TABLE'),
                    'Key': {
                        'group_id': {'S': group_id}
                    }
                }
            }
    transact_items.append(delete_group)
    
    #################################################
    # デバイス管理テーブル更新用オブジェクト作成
    #################################################
    device_group_list = []
    for item in device_list:
        #グループID取得(デバイス管理テーブル)
        device_info = ddb.get_device_info(item,device_table).get('Items',{})
        if len(device_info)==0:
            continue
        device_group_list = device_info[0].get('device_data',{}).get('config',{}).get('group_list',[])
        imei = device_info[0].get('imei',"")
        try:
            device_group_list.remove(group_id)
        except ValueError:
            pass
        
        device_update_expression = f'SET #device_data_attr.#config_attr.#group_list_attr = :group_list'
        device_expression_attribute_values = {
            ':group_list': device_group_list
        }
        device_expression_attribute_name = {
            '#device_data_attr': device_data_attr,
            '#config_attr': config_attr,
            '#group_list_attr' : group_list_attr
        }
        device_expression_attribute_values_fmt = convert.dict_dynamo_format(device_expression_attribute_values)
        
        update_device = {
            'Update': {
                            'TableName': parameter.get('DEVICE_TABLE'),
                            'Key': {
                                'device_id': {'S': item},
                                'imei':{'S':imei}
                            },
                            'UpdateExpression': device_update_expression,
                            'ExpressionAttributeValues': device_expression_attribute_values_fmt,
                            'ExpressionAttributeNames' : device_expression_attribute_name
                        }
        }

        transact_items.append(update_device)

    #################################################
    # ユーザ管理テーブル更新用オブジェクト作成
    #################################################
    user_group_list = []
    for item in user_list:
        #グループID取得(ユーザ管理テーブル)
        user_info = ddb.get_device_info(item,user_table).get('Item',{})
        user_group_list = user_info.get('user_data',{}).get('group_list',[])
        try:
            user_group_list.remove(group_id)
        except ValueError:
            pass
        
        user_update_expression = f'SET #user_data_attr.#group_list_attr = :group_list'
        user_expression_attribute_values = {
            ':group_list': user_group_list
        }
        user_expression_attribute_name = {
            '#user_data_attr': user_data_attr,
            '#group_list_attr' : group_list_attr
        }
        user_expression_attribute_values_fmt = convert.dict_dynamo_format(user_expression_attribute_values)
        
        update_user = {
            'Update': {
                            'TableName': parameter.get('USER_TABLE'),
                            'Key': {
                                'user_id': {'S': item}
                            },
                            'UpdateExpression': user_update_expression,
                            'ExpressionAttributeValues': user_expression_attribute_values_fmt,
                            'ExpressionAttributeNames' : user_expression_attribute_name
                        }
        }

        transact_items.append(update_user)


    #################################################
    # 契約管理テーブル更新用オブジェクト作成
    #################################################
    contract_group_list = ddb.get_contract_info(contract_id,contract_table).get('Item',{}).get('contract_data',{}).get('group_list',{})
    try:
        contract_group_list.remove(group_id)
    except ValueError:
        pass
    contract_update_expression = f'SET #map.#group_list_attr = :group_list'
    contract_expression_attribute_values = {
        ':group_list': contract_group_list
    }
    contract_expression_attribute_name = {
        '#map': contract_data_attr,
        '#group_list_attr' : group_list_attr
    }
    contract_expression_attribute_values_fmt = convert.dict_dynamo_format(contract_expression_attribute_values)
    
    update_contract = {                  
                        'Update': {
                            'TableName': parameter.get('CONTRACT_TABLE'),
                            'Key': {
                                'contract_id': {'S':contract_id}
                            },
                            'UpdateExpression': contract_update_expression,
                            'ExpressionAttributeValues': contract_expression_attribute_values_fmt,
                            'ExpressionAttributeNames' : contract_expression_attribute_name
                        }
                    }
    transact_items.append(update_contract)
    
    #################################################
    # DB書き込みトランザクション実行
    #################################################
    transact_result = ddb.execute_transact_write_item(transact_items)

    if transact_result:
        res_body = {
            'code':'0000',
            'message':'グループの削除が完了しました。'
        }
    else:
        res_body = {
            'code':'9999',
            'message':'グループの削除に失敗しました。'
        }

    return {
        'statusCode': 200,
        'headers': res_headers,
        'body': json.dumps(res_body, ensure_ascii=False, default=convert.decimal_default_proc)
        #'body': res_body
    }