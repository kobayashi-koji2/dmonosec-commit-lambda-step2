import boto3

ssm = boto3.client("ssm", region_name="ap-northeast-1")


##################################
# パラメータストアから値を取得
##################################
def get_ssm_params(key):
    result = {}
    if isinstance(key, tuple) and len(key) > 1:
        response = ssm.get_parameters(
            Names=key,
            WithDecryption=True,
        )

        for p in response["Parameters"]:
            result[p["Name"]] = p["Value"]
    elif isinstance(key, str):
        response = ssm.get_parameter(Name=key, WithDecryption=True)
        result = response["Parameter"]["Value"]
    return result
