from aws_lambda_powertools import Logger
from boto3.dynamodb.conditions import Attr, Key

logger = Logger()

def get_device_info_only_pj2_by_contract_id(pk,table):

    dev_info_list = []
    start_key = None
    done = True
    
    while done:
        if start_key:
            response = table.query(
                IndexName = "contract_id_index",
                KeyConditionExpression=Key("contract_id").eq(pk),
                FilterExpression=Attr("contract_state").ne(2) & Attr("device_type").eq("PJ2"),
                Limit = 220,
                ExclusiveStartKey = start_key
            )
        else:
            response = table.query(
                IndexName = "contract_id_index",
                KeyConditionExpression=Key("contract_id").eq(pk),
                FilterExpression=Attr("contract_state").ne(2) & Attr("device_type").eq("PJ2"),
                Limit = 220
            )
            
        dev_info_list.extend(response["Items"])
        start_key = response.get('LastEvaluatedKey', None)
            
        if start_key == None:
             break
        
    return dev_info_list
