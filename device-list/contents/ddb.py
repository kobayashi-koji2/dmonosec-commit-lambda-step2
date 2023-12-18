import boto3
import os
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from operator import itemgetter
region_name = os.environ.get('AWS_REGION')

dynamodb = boto3.resource('dynamodb')
client = boto3.client('dynamodb', region_name=region_name)

#デバイス情報取得(契約状態:使用不可以外)
def get_device_info(pk,table):
    response = table.query(
        KeyConditionExpression=Key('device_id').eq(pk),
        FilterExpression=Attr('contract_state').ne(2)
    )
    return response

#登録前デバイス取得
def get_pre_reg_device_info(pk,table):
    pre_reg_device_list = []
    response = table.query(
        IndexName='contract_id_index',
        KeyConditionExpression=Key('contract_id').eq(pk)
    ).get("Items", [])
    for items in response:
        #レスポンス生成(未登録デバイス)
        pre_reg_device_list.append({
            'imei':items['imei'],
            'device_registration_datetiime':items['dev_reg_date']    
        })
        
    #return sorted(pre_reg_device_list, key=itemgetter('dev_reg_datetime'))
    return pre_reg_device_list