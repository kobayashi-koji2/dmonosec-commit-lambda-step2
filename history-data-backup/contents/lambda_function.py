from __future__ import print_function
import json
import base64
import logging
import traceback
from aws_lambda_powertools import Logger
 
null = None

logger = Logger()


def verify_if_expired(payload):
    parsed_json = json.loads(payload)
    if str(parsed_json["eventName"]).upper() == "REMOVE":
        user_identity = parsed_json.get("userIdentity", {})
        if user_identity and user_identity.get("principalId", {}) == "dynamodb.amazonaws.com":
            return True
    return False


def lambda_handler(event, context):
    try:
        output = []
        for record in event['records']:
            payload = base64.b64decode(record['data']).decode('utf8')
            logger.debug(f"payload={payload}")

            # TTL削除判定
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

    except Exception as e:
        logger.error(f"Exception: {e}")
        logger.error(traceback.format_exc())
 