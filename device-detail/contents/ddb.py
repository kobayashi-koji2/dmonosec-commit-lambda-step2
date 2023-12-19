import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

dynamodb = boto3.resource('dynamodb')

#デバイス情報取得(契約状態:使用不可以外)
def get_device_info(pk,table):
    response = table.query(
        KeyConditionExpression=Key('device_id').eq(pk),
        FilterExpression=Attr('contract_state').ne(2)
    )
    return response