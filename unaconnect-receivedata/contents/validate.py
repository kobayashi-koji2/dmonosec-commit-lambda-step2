import os
from aws_lambda_powertools import Logger

logger = Logger()


def validate(req_body):
    if not req_body.get("dataType") or req_body.get("dataType") == "":
        return {"message": "data_typeがありません"}
    
    if not req_body.get("deviceId") or req_body.get("deviceId") == "":
        return {"message": "sigfox_idがありません"}
    
    return {}