import json

from aws_lambda_powertools import Logger

import ddb

logger = Logger()


# パラメータチェック
def validate(event, contract_table, device_relation_table, remote_controls_table):
    body_params = json.loads(event.get("body", "{}"))
    if "device_req_no" not in body_params:
        return {"code": "9999", "message": "パラメータが不正です"}

    device_req_no = body_params.get("device_req_no")
    remote_control = ddb.get_remote_control_info(device_req_no, remote_controls_table)
    if not remote_control:
        return {"code": "9999", "message": "端末要求番号が存在しません。"}

    params = {
        "device_req_no": device_req_no,
    }

    return {
        "code": "0000",
        "request_params": params,
        "remote_control": remote_control,
    }
