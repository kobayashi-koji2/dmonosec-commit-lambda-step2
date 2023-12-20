import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')

#デバイス情報取得(デバイスID+IMEI)
def get_device_info_by_id_imei(pk, sk, table):
    response = table.get_item(Key={'device_id': pk, 'imei': sk})
    return response


#デバイス設定更新
def update_device_settings(device_id,imei,device_settings,table):
    map_attribute_name = 'device_data'
    sub_attribute_name1 = 'config'
    sub_attribute_name2 = 'terminal_settings'
    device_name = device_settings.get('device_name',{})
    di_new_val = device_settings.get('di_list',{})
    do_new_val = device_settings.get('do_list',{})
    ai_new_val = device_settings.get('ai_list',{})
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
    table.update_item(
        Key={
            'device_id': device_id,
            'imei':imei
        },
        UpdateExpression=update_expression,
        ExpressionAttributeValues=expression_attribute_values,
        ExpressionAttributeNames=expression_attribute_name
    )
    return