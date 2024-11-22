# layer
import db

# パラメータチェック
def validate(contract_id, contract_table):
    contract_info = db.get_contract_info(contract_id, contract_table)
    if contract_info:
        return {"message": "既に契約があります"}
    return {}
