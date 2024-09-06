import boto3
import os
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from operator import itemgetter
from aws_lambda_powertools import Logger

logger = Logger()
region_name = os.environ.get("AWS_REGION")

dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb", region_name=region_name)


# デバイス情報取得(契約状態:使用不可以外)
def get_device_info(pk, table):
    response = table.query(
        KeyConditionExpression=Key("device_id").eq(pk),
        FilterExpression=Attr("contract_state").ne(2),
    )
    return response


# 登録前デバイス取得
def get_pre_reg_device_info(pk, table):
    pre_reg_device_list = []
    response = table.query(
        IndexName="contract_id_index", KeyConditionExpression=Key("contract_id").eq(pk)
    ).get("Items", [])
    for items in response:
        # レスポンス生成(未登録デバイス)
        pre_reg_device_list.append(
            {
                "device_imei": items["imei"],
                "device_registration_datetime": items["dev_reg_datetime"],
                "device_code": items["device_code"],
            }
        )

    # return sorted(pre_reg_device_list, key=itemgetter('dev_reg_datetime'))
    return pre_reg_device_list

def get_device_info_by_contract_id(pk,table):

    dev_info_list = []
    start_key = None
    done = True
    
    while done:
        
        if start_key:
            response = table.query(
                IndexName = "contract_id_index",
                KeyConditionExpression=Key("contract_id").eq(pk),
                Limit = 220,
                ExclusiveStartKey = start_key
            )
        else:
            response = table.query(
                IndexName = "contract_id_index",
                KeyConditionExpression=Key("contract_id").eq(pk),
                Limit = 220
            )

        #logger.info(f"レスポンス:{response["Items"]}")
            
        dev_info_list.extend(response["Items"])
        start_key = response.get('LastEvaluatedKey', None)
            
        if start_key == None:
             break
        
    return dev_info_list
