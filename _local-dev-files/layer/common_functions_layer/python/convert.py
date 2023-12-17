from decimal import Decimal
from jose import jwt


# idトークンデコード
def decode_idtoken(event):
    headers = event.get("headers", {})
    if not headers or "Authorization" not in headers:
        return False
    idtoken = event["headers"]["Authorization"]
    decoded_idtoken = jwt.get_unverified_claims(idtoken)
    return decoded_idtoken


# dict型をDynamoDBで使える形式に変換
def dict_dynamo_format(dict):
    ret_dict = {}
    for k, v in dict.items():
        print(k, v)
        ret_dict[k] = to_dynamo_format(v)
    return ret_dict


def to_dynamo_format(v):
    if type(v) is str:
        return {"S": v}
    if type(v) is int:
        return {"N": str(v)}
    if type(v) is bool:
        return {"BOOL": v}
    if type(v) is list:
        return {"L": [to_dynamo_format(a) for a in v]}
    if type(v) is dict:
        return {"M": dict_dynamo_format(v)}


# dict型のDecimalを数値に変換
def float_to_decimal(param):
    if isinstance(param, dict):
        for key, value in param.items():
            if isinstance(value, float):
                param[key] = Decimal(str(value))
            else:
                float_to_decimal(value)
    elif isinstance(param, list):
        for item in param:
            float_to_decimal(item)
    return param


def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError
