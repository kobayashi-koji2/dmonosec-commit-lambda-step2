from __future__ import print_function
import json
import base64
import logging
from aws_lambda_powertools import Logger
 
null = None

logger = Logger()


def lambda_handler(event, context):
    output = []
    for record in event['records']:
        payload = base64.b64decode(record['data']).decode('utf8')
        logger.debug(f"payload={payload}")
 
        ret_val = verify_if_expired(payload)
        if ret_val == True:
            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': record['data']
            }
            output.append(output_record)
 
        else:
            output_record = {
                'recordId': record['recordId'],
                'result': 'Dropped',
                'data': null
            }
            output.append(output_record)
 
    return {'records': output}
 
 
def verify_if_expired(payload):
    try:
        parsed_json = json.loads(payload)
        if str(parsed_json["eventName"]).upper() == "REMOVE":
            if parsed_json["userIdentity"]["principalId"] == "dynamodb.amazonaws.com":
                return True
    except Exception as e:
        print(e,"error")
    return False
