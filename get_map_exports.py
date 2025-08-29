import asyncio
import json
import os

import httpx
from httpx_retries import Retry, RetryTransport

env_vars = os.environ
base_url = "https://api.raptormaps.com"

retry = Retry(total=5, backoff_factor=0.75)


def get_bearer_token(client_secret, client_id):
    url = 'https://api.raptormaps.com/oauth/token'
    headers = {'content-type': 'application/json'}
    body = {
        'client_id': client_id,
        'client_secret': client_secret,
        'audience': 'api://customer-api'}

    token_response = httpx.post(
        url,
        headers=headers,
        data=json.dumps(body))

    response_data = token_response.json()
    return response_data.get('access_token')


bearer_token = get_bearer_token(
    env_vars['CLIENT_SECRET'], env_vars['CLIENT_ID'])
headers = {'Authorization': f"Bearer {bearer_token}"}


async def get_map_extracts(inspection_id, client):
    output = f'{os.getcwd()}/{inspection_id}.zip'
    url = f'{base_url}/v2/solar_inspections/{inspection_id}/exports/map_exports?org_id={env_vars["ORG_ID"]}'
    res = await client.get(url, headers=headers, timeout=httpx.Timeout(timeout=10, read=600.0, pool=180.0))
    if res.status_code not in [200, 204]:
        print(res.status_code)
        res.raise_for_status()
    if res.status_code == 200:
        with open(output, 'wb') as f:
            f.write(res.content)


def get_all_farms():
    farm_ids = []
    result_list_len = 1
    i = 0
    with httpx.Client() as client:
        while result_list_len != 0:
            offset = i*100
            url = f'{base_url}/v2/solar_farms?org_id={env_vars["ORG_ID"]}&offset={offset}'
            res = client.get(url, headers=headers, timeout=httpx.Timeout(
                timeout=10, read=600.0, pool=180.0))
            if res.status_code != 200:
                print(res.json())
                res.raise_for_status()
            farms_list = res.json()
            result_list_len = len(farms_list)
            for item in farms_list:
                farm_ids.append(item["id"])
            i += 1

    return farm_ids


async def get_latest_inspection(farm_id, client):
    latest_inspection_id = 0
    url = f'{base_url}/v2/solar_farms/{farm_id}/solar_inspections?org_id={env_vars["ORG_ID"]}'
    res = await client.get(url, headers=headers, timeout=httpx.Timeout(timeout=10, read=600.0, pool=180.0))
    if res.status_code != 200:
        res.raise_for_status()
    if len(res.json()) > 0:
        sorted_inspections = sorted(
            res.json(), key=lambda x: x['updated_tsecs'], reverse=True)
        latest_inspection_id = sorted_inspections[0]['id']

    return latest_inspection_id


async def get_new_inspection_list():
    inspection_ids = []
    transport = RetryTransport(retry=retry)
    farm_ids = get_all_farms()
    async with httpx.AsyncClient(transport=transport) as client:
        pending = []
        for item in farm_ids:
            pending.append(get_latest_inspection(item, client))
        results_list = await asyncio.gather(*pending, return_exceptions=True)
        for result in results_list:
            if isinstance(result, BaseException):
                print(result)
                raise result
            insp_id = result
            if insp_id != 0:
                inspection_ids.append(insp_id)
        pending.clear()
        for inspection_id in inspection_ids:
            pending.append(get_map_extracts(inspection_id, client))
        results_list = await asyncio.gather(*pending, return_exceptions=True)
        for result in results_list:
            if isinstance(result, BaseException):
                print(result)
                raise result


if __name__ == "__main__":
    id_list = asyncio.run(get_new_inspection_list())
