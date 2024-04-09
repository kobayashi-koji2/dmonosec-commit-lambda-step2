import json
import urllib

from aws_lambda_powertools import Logger

# layer
import ddb
import ssm

logger = Logger()

SORACOM_ENDPOINT = "https://api.soracom.io/v1"
SORACOM_G_ENDPOINT = "https://g.api.soracom.io/v1"


def get_soracom_token(operator_table):
    # 1. ソラコム接続用のキー取得
    soracom_info = ddb.get_opid_info(operator_table)
    data = {
        "authKeyId": ssm.get_ssm_params(soracom_info["auth_key"]),
        "authKey": ssm.get_ssm_params(soracom_info["secret"]),
    }
    # 2. 認証処理
    req = urllib.request.Request(
        url=f"{SORACOM_ENDPOINT}/auth",
        headers={"Content-Type": "application/json"},
        data=json.dumps(data).encode(),
    )
    # # ローカル動作確認用
    # logger.debug(req)
    # return {"apiKey": "1234567890123456789", "token": "jeukdki0999"}
    # #
    with urllib.request.urlopen(req, timeout=10) as res:
        return json.loads(res.read().decode())


# IMSI情報取得API
def get_imsi_info(token, iccid):
    # 3. ICCIDからIMSIを取得
    headers = {
        "Content-Type": "application/json",
        "X-Soracom-API-Key": token["apiKey"],
        "X-Soracom-Token": token["token"],
    }
    apiurl = f"/sims/{iccid}"
    body = dict()
    method = "GET"

    coverage_url = SORACOM_ENDPOINT
    url = coverage_url + apiurl
    code, result = exe_soracom_api(url, headers, body, method)
    if code != 200:
        coverage_url = SORACOM_G_ENDPOINT
        url = coverage_url + apiurl
        code, result = exe_soracom_api(url, headers, body, method)
        if code != 200:
            return {"message": "IMSI情報取得APIのリクエストに失敗しました。"}

    res = json.loads(result)
    subscribers = res["profiles"][iccid]["subscribers"]
    imsi = next(iter(subscribers))

    return {"imsi": imsi, "coverage_url": coverage_url}


def imei_lock(token, imei, sim_id, coverage_url):
    # 1. IMEIロック
    headers = {
        "Content-Type": "application/json",
        "X-Soracom-API-Key": token["apiKey"],
        "X-Soracom-Token": token["token"],
    }
    apiurl = f"/sims/{sim_id}/set_imei_lock"
    body = {"imei": imei}
    method = "POST"

    url = coverage_url + apiurl
    code, result = exe_soracom_api(url, headers, body, method)
    if code != 200:
        return {"message": "IMEIロックAPIのリクエストに失敗しました。"}

    return {}


def cancel_lock(token, sim_id, coverage_url):
    # 2. 解約ロック
    headers = {
        "Content-Type": "application/json",
        "X-Soracom-API-Key": token["apiKey"],
        "X-Soracom-Token": token["token"],
    }
    apiurl = f"/sims/{sim_id}/disable_termination"
    body = dict()
    method = "POST"

    url = coverage_url + apiurl
    code, result = exe_soracom_api(url, headers, body, method)
    if code != 200:
        return {"message": "解約ロックAPIのリクエストに失敗しました。"}

    return {}


def set_group(token, sim_id, group_id, coverage_url):
    headers = {
        "Content-Type": "application/json",
        "X-Soracom-API-Key": token["apiKey"],
        "X-Soracom-Token": token["token"],
    }
    apiurl = f"/sims/{sim_id}/set_group"
    body = {"groupId": group_id}
    method = "POST"

    url = coverage_url + apiurl
    code, result = exe_soracom_api(url, headers, body, method)
    if code != 200:
        return {"message": "グループ設定APIのリクエストに失敗しました。"}

    return {}


def exe_soracom_api(url, headers, body, method):
    logger.debug(f"ApiRequest: url={url}, headers={headers}, body={body}, method={method}")
    # # ローカル動作確認用
    # res = {"profiles": {"2022121200100000230": {"subscribers": ["imsi0", "imsi1", "imsi2"]}}}
    # return 200, json.dumps(res).encode()
    # #
    try:
        req = urllib.request.Request(
            url=url, headers=headers, data=json.dumps(body).encode(), method=method
        )
        with urllib.request.urlopen(req, timeout=10) as res:
            return res.status, res.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.reason
    except urllib.error.URLError as err:
        return err.code, err.reason
