import os
from aws_lambda_powertools import Logger

logger = Logger()

RECV_PAST_TIME = int(os.environ["RECV_PAST_TIME"])
RECV_FUTURE_TIME = int(os.environ["RECV_FUTURE_TIME"])

def validate(event):
    if not event.get("dataType") or event.get("dataType") == "":
        return {"message": "data_typeがありません"}
    
    if not event.get("deviceId") or event.get("deviceId") == "":
        return {"message": "sigfox_idがありません"}
    
    return {}