import json
import os
import boto3
import validate
from aws_lambda_powertools import Logger

import ssm
import db
import convert

logger = Logger()
dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))

def lambda_handler(event, context):
    logger.info(event)
    try:
        res_headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        }
        # DynamoDB操作オブジェクト生成
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        body = json.loads(event.get("body", {}))
        contract_id = body.get("contract_id")
        mss_office = body.get("mss_office")
        use = body.get("use")

        validate_result = validate.validate(contract_id, contract_table)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }
        
        transact_items = []
        put_item = {
            "contract_id": contract_id,
            "service": "monosc",
            "contract_data": {
                "user_list": [],
                "device_list": [],
                "group_list": []
            },
            "use": use,
            "mss_office": mss_office,
            "history_storage_period": 3,
            "contract": 1
        }
        put_item_fmt = convert.dict_dynamo_format(put_item)
        put_contract = {
            "Put": {
                "TableName": ssm.table_names["CONTRACT_TABLE"],
                "Item": put_item_fmt,
            }
        }
        transact_items.append(put_contract)
        logger.debug(f"put_item: {put_contract}")

        # 各データを登録・更新・削除
        if not db.execute_transact_write_item(transact_items):
            res_body = {"message": "DynamoDBへの更新処理に失敗しました。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }
        res_body = {
            "data": put_item
        }
        return {
            'statusCode': 200,
            "headers": res_headers,
            'body': json.dumps(res_body, ensure_ascii=False)
        }
    
    except Exception:
        logger.error("予期しないエラー", exc_info=True)
        body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(body, ensure_ascii=False),
        }