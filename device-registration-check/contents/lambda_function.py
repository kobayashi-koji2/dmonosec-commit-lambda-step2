import datetime
import os
import time
import uuid
import zoneinfo

import boto3
from aws_lambda_powertools import Logger
from aws_xray_sdk.core import patch_all
from dateutil import relativedelta

import convert
import db
import ddb
import ssm

patch_all()

logger = Logger()

dynamodb = boto3.resource("dynamodb", endpoint_url=os.environ.get("endpoint_url"))


def lambda_handler(event, context):
    contract_table = dynamodb.Table(ssm.table_names["CONTRACT_TABLE"])
    pre_register_device_table = dynamodb.Table(ssm.table_names["PRE_REGISTER_DEVICE_TABLE"])
    device_announcement_table = dynamodb.Table(ssm.table_names["DEVICE_ANNOUNCEMENT_TABLE"])

    # 7日前の登録前デバイスを取得
    today = datetime.datetime.now(zoneinfo.ZoneInfo("Asia/Tokyo")).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    logger.info({"today": today})
    low_dev_reg_datetime = int((today - datetime.timedelta(days=7)).timestamp() * 1000)
    high_dev_reg_datetime = int((today - datetime.timedelta(days=6)).timestamp() * 1000) - 1
    logger.info(
        {
            "low_dev_reg_datetime": low_dev_reg_datetime,
            "high_dev_reg_datetime": high_dev_reg_datetime,
        }
    )

    pre_register_device_list = ddb.get_pre_register_device_list(
        pre_register_device_table, low_dev_reg_datetime, high_dev_reg_datetime
    )
    logger.info({"pre_register_device_list": pre_register_device_list})

    if not pre_register_device_list:
        logger.info("対象の登録前デバイス無し")
        return

    # デバイス登録
    error_pre_device_list = []
    for pre_device in pre_register_device_list:
        try:
            contract = db.get_contract_info(pre_device["contract_id"], contract_table)
            device_announcements = ddb.get_device_announcement_list(
                device_announcement_table, pre_device["identification_id"]
            )
            _register_device(pre_device, contract, device_announcements)
        except Exception:
            logger.error(pre_device, exc_info=True)
            error_pre_device_list.append(pre_device)

    if error_pre_device_list:
        logger.error({"error_pre_device_list": error_pre_device_list})
        raise Exception("登録に失敗したデバイスあり")


def _register_device(pre_device, contract, device_announcements):
    device_id = str(uuid.uuid4())
    logger.info({"device_id": device_id})

    transact_items = []

    # デバイス種別の判定
    if pre_device["device_code"] == "MS-C0100":
        device_type = "PJ1"
    elif pre_device["device_code"] == "MS-C0110":
        device_type = "PJ2"
    elif pre_device["device_code"] == "MS-C0120":
        device_type = "PJ3"
    elif pre_device["device_code"] == "MS-C0130":
        device_type = "UnaTag"

    di_num = 0
    do_num = 0
    if device_type == "PJ1":
        di_num = 1
        do_num = 0
    elif device_type == "PJ2":
        di_num = 8
        do_num = 2

    # デバイス情報登録
    if device_type == "UnaTag":
        device_item = {
            "device_id": device_id,
            "identification_id": pre_device["identification_id"],
            "contract_state": 0,
            "device_type": device_type,
            "contract_id": pre_device["contract_id"],
            "device_data": {
                "param": {
                    "contract_id": pre_device["contract_id"],
                    "iccid": "",
                    "imsi": "",
                    "device_code": pre_device["device_code"],
                    "dev_reg_datetime": pre_device["dev_reg_datetime"],
                    "dev_use_reg_datetime": int(time.time() * 1000),
                    "service": "monosc",
                    "use_type": "0",
                    "coverage_url": pre_device["coverage_url"],
                },
                "config": {
                    "device_name": None,
                    "device_healthy_period": 3,
                    "terminal_settings": {
                        "di_list": [],
                        "do_list": [],
                    },
                },
            },
        }
    else:
        device_item = {
            "device_id": device_id,
            "identification_id": pre_device["identification_id"],
            "contract_state": 0,
            "device_type": device_type,
            "contract_id": pre_device["contract_id"],
            "device_data": {
                "param": {
                    "contract_id": pre_device["contract_id"],
                    "iccid": pre_device["iccid"],
                    "imsi": pre_device["imsi"],
                    "device_code": pre_device["device_code"],
                    "dev_reg_datetime": pre_device["dev_reg_datetime"],
                    "dev_use_reg_datetime": int(time.time() * 1000),
                    "service": "monosc",
                    "use_type": "0",
                    "coverage_url": pre_device["coverage_url"],
                },
                "config": {
                    "device_name": None,
                    "device_healthy_period": 3,
                    "terminal_settings": {
                        "di_list": [
                            {
                                "di_no": di_no,
                                "di_name": f"接点入力{di_no}",
                                "di_on_name": "Close",
                                "di_on_icon": "on",
                                "di_off_name": "Open",
                                "di_off_icon": "off",
                            }
                            for di_no in range(1, di_num + 1)
                        ],
                        "do_list": [
                            {
                                "do_no": do_no,
                                "do_name": f"接点出力{do_no}",
                                "do_control": None,
                                "do_specified_time": None,
                                "do_di_return": 0,
                                "do_timer_list": [],
                            }
                            for do_no in range(1, do_num + 1)
                        ],
                    },
                },
            },
        }
    logger.info({"device_item": device_item})
    transact_items.append(
        {
            "Put": {
                "TableName": ssm.table_names["DEVICE_TABLE"],
                "Item": convert.dict_dynamo_format(device_item),
            }
        }
    )

    # 契約情報更新
    device_list = contract.get("contract_data", {}).get("device_list", [])
    device_list.append(device_id)
    transact_items.append(
        {
            "Update": {
                "TableName": ssm.table_names["CONTRACT_TABLE"],
                "Key": {"contract_id": {"S": pre_device["contract_id"]}},
                "UpdateExpression": "SET contract_data.device_list = :d",
                "ExpressionAttributeValues": {":d": convert.to_dynamo_format(device_list)},
            }
        }
    )
    if device_type == "UnaTag":
        #sigfox_id登録
        sigfox_id_item = {
            "sigfox_id": pre_device["identification_id"],
            "contract_id": pre_device["contract_id"],
            "device_id": device_id,
        }
        transact_items.append(
            {
                "Put": {
                    "TableName": ssm.table_names["SIGFOX_ID_TABLE"],
                    "Item": convert.dict_dynamo_format(sigfox_id_item),
                }
            }
        )
    else:
        # IMEI,ICCID登録
        imei_item = {
            "imei": pre_device["identification_id"],
            "contract_id": pre_device["contract_id"],
            "device_id": device_id,
        }
        transact_items.append(
            {
                "Put": {
                    "TableName": ssm.table_names["IMEI_TABLE"],
                    "Item": convert.dict_dynamo_format(imei_item),
                }
            }
        )

        iccid_item = {
            "iccid": pre_device["iccid"],
            "contract_id": pre_device["contract_id"],
            "device_id": device_id,
        }
        transact_items.append(
            {
                "Put": {
                    "TableName": ssm.table_names["ICCID_TABLE"],
                    "Item": convert.dict_dynamo_format(iccid_item),
                }
            }
        )

    # 登録前デバイス削除
    transact_items.append(
        {
            "Delete": {
                "TableName": ssm.table_names["PRE_REGISTER_DEVICE_TABLE"],
                "Key": {"identification_id": {"S": pre_device["identification_id"]}},
            }
        }
    )

    # デバイス関連お知らせ情報削除
    if device_announcements:
        transact_items.append(
            {
                "Delete": {
                    "TableName": ssm.table_names["DEVICE_ANNOUNCEMENT_TABLE"],
                    "Key": {
                        "device_announcement_id": {
                            "S": device_announcements.get("device_announcement_id")
                        }
                    },
                }
            }
        )

    # デバイス関連お知らせ情報追加
    announcement_create_datetime = int(time.time() * 1000)
    expire_datetime = int(
        (
            datetime.datetime.fromtimestamp(announcement_create_datetime / 1000)
            + relativedelta.relativedelta(months=1)
        ).timestamp()
    )
    device_announcement_item = {
        "device_announcement_id": str(uuid.uuid4()),
        "contract_id": pre_device["contract_id"],
        "announcement_create_datetime": announcement_create_datetime,
        "identification_id": pre_device["identification_id"],
        "device_code": pre_device["device_code"],
        "device_announcement_type": "auto_regist_complete",
        "expire_datetime": expire_datetime,
    }
    logger.info({"device_announcement_item": device_announcement_item})
    transact_items.append(
        {
            "Put": {
                "TableName": ssm.table_names["DEVICE_ANNOUNCEMENT_TABLE"],
                "Item": convert.dict_dynamo_format(device_announcement_item),
            }
        }
    )

    if not db.execute_transact_write_item(transact_items):
        logger.error("デバイス登録に失敗")
        raise Exception("デバイス登録に失敗")
