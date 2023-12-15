#dict型をDynamoDBで使える形式に変換
def dict_dynamo_format(dict):
    ret_dict = {}
    for k, v in dict.items():
        print(k,v)
        ret_dict[k] = to_dynamo_format(v)
    return ret_dict
    
def to_dynamo_format(v):
    if type(v) is str:
        return {'S': v}
    if type(v) is int:
        return {'N': str(v)}
    if type(v) is bool:
        return {'BOOL': v}
    if type(v) is list:
        return {'L': [to_dynamo_format(a) for a in v]}
    if type(v) is dict:
        return {'M': dict_dynamo_format(v)}

def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError