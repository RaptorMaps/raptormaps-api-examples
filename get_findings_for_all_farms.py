import asyncio
import csv
import json
import os

import httpx
from httpx_retries import Retry, RetryTransport

env_vars = os.environ
base_url = "https://api.raptormaps.com"
file_name = 'inspection_findings.csv'
fieldnames = ['anomaly', 'anomaly_count', 'est_affected_dc_kw', 'est_affected_dc_percent',
              'est_annual_impact_kw_h', 'est_annual_impact_dollars', 'module_count', "farm_name", "inspection_id"]


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


def write_findings_to_csv(findings):
    with open(file_name, 'a', newline='') as csvfile:
        writer = csv.DictWriter(csvfile)
        for finding in findings:
            writer.writerow(finding)

# Get findings from latest inspections and write data to CSV.


async def get_inspection_findings(farm_inspection, client):
    inspection_id = farm_inspection["inspection_id"]
    url = f'{base_url}/v2/solar_inspections/{inspection_id}/findings?org_id={env_vars["ORG_ID"]}'
    res = await client.get(url, headers=headers, timeout=httpx.Timeout(timeout=10, read=600.0, pool=180.0))
    if res.status_code not in [200, 204]:
        print(res.status_code)
        res.raise_for_status()
    if res.status_code == 200:
        findings = res.json()
        for finding in findings:
            finding["farm_name"] = farm_inspection["farm_name"]
            finding["inspection_id"] = inspection_id
        return findings


async def get_all_farms():
    farm_ids = []
    result_list_len = 1
    i = 0
    async with httpx.AsyncClient() as client:
        while result_list_len != 0:
            offset = i*100
            url = f'{base_url}/v2/solar_farms?org_id={env_vars["ORG_ID"]}&offset={offset}'
            res = await client.get(url, headers=headers, timeout=httpx.Timeout(timeout=10, read=600.0, pool=180.0))
            if res.status_code != 200:
                print(res.json())
                res.raise_for_status()
            farms_list = res.json()
            result_list_len = len(farms_list)
            for item in farms_list:
                farm_object = {
                    "id": item["id"],
                    "name": item["name"],
                }
                farm_ids.append(farm_object)
            i += 1

    return farm_ids


async def get_latest_inspection(farm, client):
    latest_inspection_id = 0
    farm_id = farm["id"]
    url = f'{base_url}/v2/solar_farms/{farm_id}/solar_inspections?org_id={env_vars["ORG_ID"]}'
    res = await client.get(url, headers=headers, timeout=httpx.Timeout(timeout=10, read=600.0, pool=180.0))
    if res.status_code != 200:
        res.raise_for_status()
    if len(res.json()) > 0:
        sorted_inspections = sorted(
            res.json(), key=lambda x: x['updated_tsecs'], reverse=True)
        latest_inspection_id = sorted_inspections[0]['id']

    farm_inspection = {
        "farm_name": farm["name"],
        "inspection_id": latest_inspection_id
    }
    return farm_inspection


async def create_findings_file():
    farm_inspections_list = []
    transport = RetryTransport(retry=retry)
    farm_ids = await get_all_farms()
    with open(file_name, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        csvfile.close()
    async with httpx.AsyncClient(transport=transport) as client:
        pending = []
        for item in farm_ids:
            pending.append(get_latest_inspection(item, client))
        results_list = await asyncio.gather(*pending, return_exceptions=True)
        for result in results_list:
            if isinstance(result, BaseException):
                print(result)
                raise result
            farm_inspection = result
            if farm_inspection["inspection_id"] != 0:
                farm_inspections_list.append(farm_inspection)
        pending.clear()
        for farm_inspection in farm_inspections_list:
            pending.append(get_inspection_findings(farm_inspection, client))
        results_list = await asyncio.gather(*pending, return_exceptions=True)
        write_findings_to_csv(results_list)
        print(f"All Finished. Find your results at {file_name}")


if __name__ == "__main__":
    id_list = asyncio.run(create_findings_file())
