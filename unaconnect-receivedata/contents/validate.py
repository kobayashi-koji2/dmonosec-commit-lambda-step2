import os
from aws_lambda_powertools import Logger

logger = Logger()


def validate(rec_body):
    if not rec_body.get("dataType") or rec_body.get("dataType") == "":
        return {"message": "data_typeがありません"}
    
    if not rec_body.get("deviceId") or rec_body.get("deviceId") == "":
        return {"message": "sigfox_idがありません"}
    
    return {}