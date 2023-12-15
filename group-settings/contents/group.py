import uuid
import db
import convert

def generate_group_id():
    return str(uuid.uuid4()).replace('-', '')

def create_group_info(group_info,contract_table,device_table):
    #グループID生成
    group_id = generate_group_id()
    #トランザクション書き込み用オブジェクト
    transact_items,put_group,update_contract,update_device = [],[],[],[]
    #テーブル更新用キー
    contract_data_attr = 'contract_data'
    device_data_attr = 'device_data'
    config_attr = 'config'
    device_list_attr = 'device_list'
    group_list_attr = 'group_list'
    #契約ID
    contract_id = 'na1234567'
    #グループ名
    group_name = group_info.get('group_name',{})
    #グループに登録するデバイス
    device_list = group_info.get('device_list',{})
    
    #################################################
    #グループ管理テーブル新規登録用オブジェクト作成
    #################################################
    item = {
            "group_id": group_id,#
            "group_data": {
             "config": {
              "group_name": group_info['group_name'],
              "device_list": device_list,
              "user_list":[]
             }
        }
    }
    item_fmt = convert.dict_dynamo_format(item)
    put_group = {
                    'Put':{
                        'TableName': 'dmonosc-ddb-t-monosec-groups',
                        'Item': item_fmt,
                    }
                }
    transact_items.append(put_group)
    
    #################################################
    #契約管理テーブル更新用オブジェクト作成
    #################################################
    contract_group_list = db.get_contract_info(contract_id,contract_table).get('Item',{}).get('contract_data',{}).get('group_list',{})
    contract_group_list.append(group_id)
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
                            'TableName': 'dmonosc-ddb-m-office-contracts',
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
    #デバイス管理テーブル更新用オブジェクト作成
    #################################################
    device_group_list = []
    for item in device_list:
        #グループID取得(デバイス管理テーブル)
        device_info = db.get_device_info(item,device_table).get('Items',{})
        if len(device_info)==0:
            continue
        devcie_group_list = device_info[0].get('device_data',{}).get('config',{}).get('group_list',[])
        imei = device_info[0].get('imei',"")
        device_group_list.append(group_id)
        print(f'デバイス管理グループ一覧:{device_group_list}')
        
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
                            'TableName': 'dmonosc-ddb-t-monosec-devices',
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
    # DB書き込みトランザクション実行
    #################################################
    transact_result = db.execute_transact_write_item(transact_items)
        
    return transact_result, group_id
    
def update_group_info(group_info,group_id,group_table,device_table):
    #トランザクション書き込み用オブジェクト
    transact_items,update_group,update_device = [],[],[]
    #テーブル更新用キー
    #contract_data_attr = 'contract_data'
    device_data_attr = 'device_data'
    group_data_attr = 'group_data'
    config_attr = 'config'
    group_name_attr = 'group_name'
    device_list_attr = 'device_list'
    group_list_attr = 'group_list'
    group_name = group_info.get('group_name',{})
    device_list = group_info.get('device_list',{})
    
    #################################################
    #グループ管理テーブル更新用オブジェクト作成
    #################################################
    group_update_expression = f"SET #group_data.#config.#group_name = :group_name,\
                              #group_data.#config.#device_list = :device_list"
    group_expression_attribute_values = {
        ':group_name': group_name,
        ':device_list': device_list
    }
    group_expression_attribute_name = {
        '#group_data': group_data_attr,
        '#config' : config_attr,
        '#group_name' : group_name_attr,
        '#device_list': device_list_attr
    }
    group_expression_attribute_values_fmt = convert.dict_dynamo_format(group_expression_attribute_values)
    
    update_group = {                  
                        'Update': {
                            'TableName': 'dmonosc-ddb-t-monosec-groups',
                            'Key': {
                                'group_id': {'S': group_id}
                            },
                            'UpdateExpression': group_update_expression,
                            'ExpressionAttributeValues': group_expression_attribute_values_fmt,
                            'ExpressionAttributeNames' : group_expression_attribute_name
                        }
                    }
    transact_items.append(update_group)
  
    #################################################
    #デバイス管理テーブル更新用オブジェクト作成
    ################################################# 
    #グループ更新前のデバイス一覧
    device_list_old = db.get_group_info(group_id,group_table).get('Item',{})
    #グループ更新後のデバイス一覧
    device_list = group_info.get('group_data',{}).get('config',{}).get('device_list',{})
    #グループから削除されたデバイス
    removed_devices = set(device_list_old) - set(device_list)
    #グループに追加されたデバイス
    added_devices = set(device_list) - set(device_list_old)
    
    for item in removed_devices:
        device_info = db.get_device_info(item,device_table).get('Items',{})
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
                            'TableName': 'dmonosc-ddb-t-monosec-devices',
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
        
    for item in added_devices:
        device_info = db.get_device_info(item,device_table).get('Items',{})
        if len(device_info)==0:
            continue
        device_group_list = device_info[0].get('device_data',{}).get('config',{}).get('group_list',[])
        imei = device_info[0].get('imei',"")
        device_group_list.append(group_id)
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
                            'TableName': 'dmonosc-ddb-t-monosec-devices',
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
    # DB書き込みトランザクション実行
    #################################################
    transact_result = db.execute_transact_write_item(transact_items)
    
    return transact_result,group_id