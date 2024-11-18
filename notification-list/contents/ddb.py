import boto3
import os
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr
from aws_lambda_powertools import Logger

logger = Logger()
region_name = os.environ.get("AWS_REGION")

dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb", region_name=region_name)


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

        #logger.info(f"レスポンス:{response["Items"]}")
            
        dev_info_list.extend(response["Items"])
        start_key = response.get('LastEvaluatedKey', None)
            
        if start_key == None:
             break
        
    return dev_info_list
