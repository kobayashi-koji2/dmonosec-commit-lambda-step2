#dict型をDynamoDBで使える形式に変換 *transact_write_items
def dict_dynamo_format(dict):
    ret_dict = {}
    for key, value in dict.items():
        print(key,value)
        ret_dict[key] = to_dynamo_format(value)
    return ret_dict
    
def to_dynamo_format(value):
    if type(value) is str:
        return {'S': value}
    if type(value) is int:
        return {'N': str(value)}
    if type(value) is bool:
        return {'BOOL': value}
    if type(value) is list:
        return {'L': [to_dynamo_format(a) for a in value]}
    if type(value) is dict:
        return {'M': dict_dynamo_format(value)}

#dict型のdecimalを数値に変換
def decimal_default_proc(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError