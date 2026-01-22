import csv
import json
import os
import sys
from datetime import datetime, timezone

import httpx

"""
For questions, reference the Raptor Maps API documentation found at "https://docs.raptormaps.com/reference/reference-getting-started"

To run this script requires a Raptor Maps Account and an installation of python. 
Follow the instructions in the attached docs to obtain necessary API credentials.

You will need an inspection ID to pass to the script when calling it. Run the GET Inspections by Farm ID route from the docs,
or find it at the end of the URL in the browser after having accessed an inspection report:

https://app.raptormaps.com/digital-twin/{uuid}/map/anomalies?inspection=<your inspection_id will be here>

To call the script, add the inspection id as an argument:
> python filtered_anomaly_csv.py <inspection_id>

The script will produce a CSV labled with the inspection ID and the current date and time as of the running of the script.
"""

base_url = "https://api.raptormaps.com"  # points to Raptor Maps public api
current = datetime.now()
org_id: int = os.environ.get('ORG_ID')
inspection_id: int = sys.argv[1]

# For translating the numerical value for priority into a text string
priority_dict = {
    "0": "None",
    '1': "Low",
    '2': "Medium",
    '3': "High",
    '4': "Completed",
    '5': "Unset",
}

# function for mapping the values in the API response to the desired headers


def map_anomaly_values(anomaly):
    utc_time = datetime.fromtimestamp(
        anomaly['created_tsecs'], tz=timezone.utc)
    item = {}
    item["Anomaly ID"] = anomaly["id"]
    item["Anomaly Type"] = anomaly["tag"]["tag"]
    item["Life Cycle Status"] = anomaly["anomaly_life_cycle"]["status"]
    item["Datetime UTC"] = utc_time
    item["RM Custom Location"] = anomaly["custom_locator_text"]
    item["Priority"] = priority_dict[str(anomaly['priority'])]

    return item

# Use the secret and ID to retrieve an authentication token from Raptor Maps system


def get_bearer_token():
    client_secret = os.environ["CLIENT_SECRET"]
    client_id = os.environ["CLIENT_ID"]
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

# Use the bearer token to retrieve the anomaly data for the desired inspection


def get_anomaly_data(inspection_id):
    headers = {'Authorization': f'Bearer {token}'}
    anomalies = httpx.get(
        f'{base_url}/v2/solar_inspections/{inspection_id}/anomalies?org_id={org_id}&include_tags=9612, 9603, 9604',
        headers=headers
    )

    return anomalies.json()['defects']


anomalies_list: list[dict] = get_anomaly_data(inspection_id)

# Write the CSV file. This will place the file in the same folder as the script.
with open(f'anamolies_for_inspection_{inspection_id}_{current}.csv', 'w', newline='') as newcsv:
    fieldnames = ["Anomaly ID", "Anomaly Type", "Life Cycle Status",
                  "Datetime UTC", "RM Custom Location", "Priority"]
    writer = csv.DictWriter(newcsv, fieldnames=fieldnames)

    writer.writeheader()
    for anomaly in anomalies_list:
        item = map_anomaly_values(anomaly)
        writer.writerow(item)
