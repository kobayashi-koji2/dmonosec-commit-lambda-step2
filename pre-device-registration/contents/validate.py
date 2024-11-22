# layer
import db
import json
from aws_lambda_powertools import Logger

logger = Logger()

# パラメータチェック
def validate(event, contract_table):

    body = json.loads(event.get("body", {}))
    device_code = body.get("device_code")
    iccid = body.get("iccid")
    imei = body.get("imei")
    imsi = body.get("imsi")
    sigfox_id = body.get("sigfox_id")
    contract_id = body.get("contract_id")
    ship_contract_id = body.get("ship_contract_id")
    coverage_url = body.get("coverage_url")

    # 必須パラメータチェック
    if device_code is None:
        return {"message": "機器コードが未指定です"}

    if contract_id is None:
        return {"message": "契約コードが未指定です"}

    contract_info = db.get_contract_info(contract_id, contract_table)
    if contract_info:
        return {"message": "該当の契約情報が存在しません"}

    if ship_contract_id is None:
        return {"message": "出庫契約コードが未指定です"}


    # 機器コード毎の必須パラメータチェック
    if device_code in ["MS-C0100", "MS-C0110", "MS-C0120"]:
        if iccid is None:
            return {"message": "ICCIDが未指定です"}
        if imei is None:
            return {"message": "IMEIが未指定です"}
        if imsi is None:
            return {"message": "IMSIが未指定です"}
        if coverage_url is None:
            return {"message": "カバレッジURLが未指定です"}
    elif device_code in ["MS-C0130"]:
        if sigfox_id is None:
            return {"message": "Sigfox IDが未指定です"}

    return {
        "device_code": device_code,
        "iccid": iccid,
        "imei": imei,
        "imsi": imsi,
        "sigfox_id": sigfox_id,
        "contract_id": contract_id,
        "ship_contract_id": ship_contract_id,
        "coverage_url": coverage_url
    }
