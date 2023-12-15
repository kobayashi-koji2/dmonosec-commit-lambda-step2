from decimal import Decimal

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