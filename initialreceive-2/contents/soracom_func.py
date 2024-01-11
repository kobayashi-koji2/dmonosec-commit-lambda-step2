import json
import ssm
import urllib.request
from aws_lambda_powertools import Logger

logger = Logger()


def get_token(coverage_url, soracom_info):
    data = {
        "authKeyId": ssm.get_ssm_params(soracom_info["auth_key"]),
        "authKey": ssm.get_ssm_params(soracom_info["secret"]),
    }
    req = urllib.request.Request(
        url=f"{coverage_url}/auth",
        headers={"Content-Type": "application/json"},
        data=json.dumps(data).encode(),
    )
    with urllib.request.urlopen(req, timeout=10) as res:
        return json.loads(res.read().decode())


def get_headers(token):
    return {
        "Content-Type": "application/json",
        "X-Soracom-API-Key": token.get("apiKey"),
        "X-Soracom-Token": token.get("token"),
    }


def soracom_sim_terminate_api(sim_id, coverage_url, soracom_info):
    # SIM解約許可
    headers = get_headers(get_token(coverage_url, soracom_info))
    apiurl = "/sims/{sim_id}/enable_terminate"
    apiurl = apiurl.replace("{sim_id}", sim_id)
    body = {}
    body = json.dumps(body)
    method = "POST"

    url = coverage_url + apiurl
    exe_soracom_api(url, headers, body, method)

    # SIM解約
    apiurl = "/sims/{sim_id}/suspend"
    apiurl = apiurl.replace("{sim_id}", sim_id)
    url = coverage_url + apiurl
    exe_soracom_api(url, headers, body, method)

    # SIM停止
    apiurl = "/sims/{sim_id}/deactivate"
    apiurl = apiurl.replace("{sim_id}", sim_id)
    url = coverage_url + apiurl
    exe_soracom_api(url, headers, body, method)

    return 0


def exe_soracom_api(url, headers, body, method):
    logger.debug(f"url={url}, headers={headers}, body={body}, method={method}")
    try:
        req = urllib.request.Request(url=url, headers=headers, data=body.encode(), method=method)
        with urllib.request.urlopen(req, timeout=10) as res:
            return res.getcode(), res.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.reason
    except urllib.error.URLError as err:
        return err.code, err.reason
