import csv
import datetime
import json
import os
import sys

import httpx

base_url = "https://api.raptormaps.com"
current = datetime.datetime.now()
inspection_id: int = sys.argv[1]


def get_bearer_token():
    client_secret = os.environ["DEMO_CLIENT_SECRET"]
    client_id = os.environ["DEMO_CLIENT_ID"]
    headers = {"content-type": "application/json"}
    url = f"{base_url}/oauth/token"
    body = {
        "client_id": client_id,
        "client_secret": client_secret,
        "audience": "api://customer-api"
    }

    token_response = httpx.post(url, headers=headers, data=json.dumps(body))

    response_data = token_response.json()
    return response_data.get("access_token")


token: str = get_bearer_token()
org_id: int = os.environ.get('DEMO_ORG_ID')


def get_anomaly_data(inspection_id):
    headers = {'Authorization': f'Bearer {token}'}
    anomalies = httpx.get(
        f'{base_url}/v2/solar_inspections/{inspection_id}/anomalies?org_id={org_id}',
        headers=headers
    )

    return anomalies.json()['defects']


anomalies_list: list[dict] = get_anomaly_data(inspection_id)

with open(f'anamolies_for_inspection_{inspection_id}_{current}.csv', 'w', newline='') as newcsv:
    fieldnames = anomalies_list[0].keys()
    writer = csv.DictWriter(newcsv, fieldnames=fieldnames)

    writer.writeheader()
    for item in anomalies_list:
        writer.writerow(item)
