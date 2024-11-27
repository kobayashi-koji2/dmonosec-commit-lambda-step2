import json
import os
import boto3
import validate
import time
import traceback
import ddb
import uuid
from datetime import datetime
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

        transact_items = []
        contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
        pre_register_table = dynamodb.Table(ssm.table_names["PRE_REGISTER_DEVICE_TABLE"])
        group_table = dynamodb.Table(ssm.table_names["GROUP_TABLE"])
        device_relation_table = dynamodb.Table(ssm.table_names["DEVICE_RELATION_TABLE"])

        validate_result = validate.validate(event, contract_table)
        if validate_result.get("message"):
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(validate_result, ensure_ascii=False),
            }
        
        now = datetime.now()
        now_unixtime = int(time.mktime(now.timetuple()) * 1000) + int(now.microsecond / 1000)
        pre_device_item = {
            "device_code": validate_result["device_code"],
            "contract_id": validate_result["contract_id"],
            "dev_reg_datetime": now_unixtime,
            "contract_state": 0,
        }

        if validate_result["device_code"] in ["MS-C0100", "MS-C0110", "MS-C0120"]:
            identification_id = validate_result["imei"]
            pre_device_item["iccid"] = validate_result["iccid"]
            pre_device_item["imsi"] = validate_result["imsi"]
            pre_device_item["coverage_url"] = validate_result["coverage_url"]
        elif validate_result["device_code"] in ["MS-C0130"]:
            identification_id = validate_result["sigfox_id"]
        pre_device_item["identification_id"] = identification_id
        pre_device_item["ship_contract_id"] = validate_result["ship_contract_id"]

        pre_device_item_fmt = convert.dict_dynamo_format(pre_device_item)
        put_pre_device = {
            "Put": {
                "TableName": pre_register_table.table_name,
                "Item": pre_device_item_fmt,
            }
        }
        transact_items.append(put_pre_device)

        # グループ情報取得
        contract_info = db.get_contract_info(validate_result["contract_id"], contract_table)
        if not contract_info:
            res_body = {"message": "契約情報が存在しません。"}
            return {
                "statusCode": 500,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        group_id_list = contract_info.get("contract_data", {}).get("group_list", [])

        group_id = ""
        for group_id in group_id_list:
            group_info = db.get_group_info(group_id, group_table)
            if group_info:
                group_name = group_info.get("group_data", {}).get("config", {}).get("group_name")
                if group_name == str(validate_result["ship_contract_id"]):
                    group_id = group_info.get("group_id")
                    break
        logger.info(f"グループ情報:{group_id}")

        if not group_id:
            group_id = str(uuid.uuid4())
            group_item = {
                "group_id": group_id,
                "group_data": {
                    "config": {
                        "contract_id": validate_result["contract_id"],
                        "group_name": str(validate_result["ship_contract_id"]),
                    }
                },
            }
            group_item_fmt = convert.dict_dynamo_format(group_item)
            put_group = {
                "Put": {
                    "TableName": group_table.table_name,
                    "Item": group_item_fmt,
                }
            }
            transact_items.append(put_group)

            #################################################
            # 契約管理テーブル更新用オブジェクト作成
            #################################################
            contract_group_list = contract_info.get("contract_data", {}).get("group_list", {})
            contract_group_list.append(group_id)
            contract_update_expression = f"SET #map.#group_list_attr = :group_list"
            contract_expression_attribute_values = {":group_list": contract_group_list}
            contract_expression_attribute_name = {
                "#map": "contract_data",
                "#group_list_attr": "group_list",
            }
            contract_expression_attribute_values_fmt = convert.dict_dynamo_format(
                contract_expression_attribute_values
            )

            update_contract = {
                "Update": {
                    "TableName": contract_table.table_name,
                    "Key": {"contract_id": {"S": validate_result["contract_id"]}},
                    "UpdateExpression": contract_update_expression,
                    "ExpressionAttributeValues": contract_expression_attribute_values_fmt,
                    "ExpressionAttributeNames": contract_expression_attribute_name,
                }
            }
            transact_items.append(update_contract)


        unregistered_device_relation_item = {
            "key1": "g-" + group_id,
            "key2": "pd-" + identification_id,
        }
        unregistered_device_relation_item_fmt = convert.dict_dynamo_format(unregistered_device_relation_item)
        put_unregistered_device_relation = {
            "Put": {
                "TableName": device_relation_table.table_name,
                "Item": unregistered_device_relation_item_fmt,
            }
        }
        transact_items.append(put_unregistered_device_relation)

        if not db.execute_transact_write_item(transact_items):
            res_body = {"message": "DynamoDBへの更新処理に失敗しました。"}
            return {
                "statusCode": 400,
                "headers": res_headers,
                "body": json.dumps(res_body, ensure_ascii=False),
            }

        pre_device_info = ddb.get_pre_reg_device_info_by_imei(identification_id, validate_result["contract_id"], pre_register_table)

        res_body = {"data": pre_device_info}
        return {
            "statusCode": 200,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        res_body = {"message": "予期しないエラーが発生しました。"}
        return {
            "statusCode": 500,
            "headers": res_headers,
            "body": json.dumps(res_body, ensure_ascii=False),
        }