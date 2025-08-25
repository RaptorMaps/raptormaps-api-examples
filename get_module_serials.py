import os
import requests

api_access_token = os.environ['BEARER_TOKEN']
auth_header = {'authorization': f'Bearer {api_access_token}'}
base_url = 'https://api.qa.raptormaps.com'
legacy_url = 'https://qa-app-legacy.raptormaps.com/api/v2'
org_id = 187

# get solar farms


def get_solar_farm_uuid_by_name(name):
    endpoint = f'{base_url}/sorted_solar_farms?org_id={org_id}&name={name}'
    resp = requests.get(url=endpoint, headers=auth_header)
    return resp.json()['farms'][0]['uuid']

# get inspections


def get_inspections_by_farm_uuid():
    solar_farm_uuid = get_solar_farm_uuid_by_name('megawatts')
    endpoint = f'{legacy_url}/solar_farms/{solar_farm_uuid}/solar_inspections?org_id={org_id}'
    resp = requests.get(url=endpoint, headers=auth_header)

    print(resp.request.url)
    print(resp.json())
# get defects

# get modules

# get module location


get_inspections_by_farm_uuid()
