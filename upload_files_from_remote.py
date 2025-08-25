import json
import os

import httpx

org_id = os.getenv("ORG_ID")
url = f"https://app.assets.raptormaps.com/api/v2/ingestor/upload_sessions?org_id={org_id}"

# function to retrieve bearer token for auth


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
    os.getenv("CLIENT_SECRET"), os.getenv("CLIENT_ID"))


def upload_files():
    payload = json.dumps({
        "data_url": [
            "URL to dataset goes here"
        ],
        "upload_session_name": "Replace This Name",
        "pipeline": "om",
        "order_id": 58836
    })
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {bearer_token}'
    }

    response = httpx.post(url, headers=headers, data=payload)

    print(response.text)
