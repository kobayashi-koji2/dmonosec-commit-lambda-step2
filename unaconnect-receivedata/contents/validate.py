import os
from aws_lambda_powertools import Logger

logger = Logger()


def validate(event):
    if not event.get("dataType") or event.get("dataType") == "":
        return {"message": "data_typeがありません"}
    
    if not event.get("deviceId") or event.get("deviceId") == "":
        return {"message": "sigfox_idがありません"}
    
    return {}