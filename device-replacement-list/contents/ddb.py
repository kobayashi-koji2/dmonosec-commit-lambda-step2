from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from aws_lambda_powertools import Logger

logger = Logger()


# 未登録デバイス取得(imei or sigfox_id)
def get_pre_reg_device_info_by_identification_id(identification_id, table):
    pre_register_device_info = table.get_item(Key={"identification_id": identification_id}).get("Item", {})
    return pre_register_device_info

def get_device_info_by_contract_id(pk,table):

    dev_info_list = []
    start_key = None
    done = True

    while done:
        if start_key:
            response = table.query(
                IndexName = "contract_id_index",
                KeyConditionExpression=Key("contract_id").eq(pk),
                FilterExpression=Attr("contract_state").ne(2),
                Limit = 220,
                ExclusiveStartKey = start_key
            )
        else:
            response = table.query(
                IndexName = "contract_id_index",
                KeyConditionExpression=Key("contract_id").eq(pk),
                FilterExpression=Attr("contract_state").ne(2),
                Limit = 220
            )
        dev_info_list.extend(response["Items"])
        start_key = response.get('LastEvaluatedKey', None)
        if start_key == None:
             break
        
    return dev_info_list
