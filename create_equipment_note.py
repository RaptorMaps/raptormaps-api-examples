#!/usr/bin/env python
"""create_equipment_note.py: A working example of how to create an equipment
note that has 1 file attachment.
An upload session is first created for the file to be uploaded. The file
is uploaded to Raptor Maps cloud storage, a note is created for the specific 
piece of equipment with a title and description, and finally the file
is associated to the note.
This script requires specifying the solar farm id and the equipment id.
Example:
> python create_equipment_note.py
"""

__copyright__ = "Raptor Maps Inc. 2023 (c)"

import json
import os

import httpx

RM_API = 'https://api.raptormaps.com'


def main():

    # INPUTS: change these!
    solar_farm_id = 123
    object_id = 123
    note_title = 'Test Note'
    note_description = 'Test Description'
    filepath = '/path/to/your/file/here.jpg'

    # API Authentication:
    # Set API token from environment variables
    api_access_token: str | None = os.environ.get('RM_API_TOKEN')

    # Set org id which can be found at https://app.raptormaps.com/account
    org_id = os.environ.get('RM_ORG_ID')

    def get_bearer_token(client_secret, client_id):
        url = f'{RM_API}/oauth/token'
        headers = {'content-type': 'application/json'}
        body = {
            'client_id': client_id,
            'client_secret': client_secret,
            'audience': 'api://customer-api.qa'}

        token_response = httpx.post(
            url,
            headers=headers,
            data=json.dumps(body))

        response_data = token_response.json()
        if token_response.status_code == 200:
            return response_data.get('access_token')
        else:
            raise Exception(
                f"Error getting bearer token: {response_data, token_response.url}")

    # If you do not already have an API token, this code snippet will get one using the function above.
    # The access token will be temporarily set in the environment.
    # See here for more info: https://docs.raptormaps.com/reference/reference-getting-started
    if not api_access_token:
        client_id = os.environ['RM_CLIENT_ID']
        client_secret = os.environ['RM_CLIENT_SECRET']
        api_access_token = get_bearer_token(client_secret, client_id)

        os.environ['RM_API_TOKEN'] = api_access_token

    # Get information about the file to upload
    filename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)  # in bytes

    # Setup header for the API calls
    headers = {
        'Authorization': f'Bearer {api_access_token}',
        'Content-Type': 'application/json'
    }

    # Create note
    note_endpoint = f'{RM_API}/solar_farms/{solar_farm_id}/equipment/{object_id}/notes?org_id={org_id}'
    note_payload = {
        "title": note_title,
        "body": note_description
    }
    note_resp = httpx.post(
        url=note_endpoint,
        headers=headers,
        data=json.dumps(note_payload)
    )
    note = note_resp.json()
    note_id = note.get('id')

    # If you do not want to upload a file, you can stop here

    # Create upload session for file
    upload_session_endpoint = f'{RM_API}/v2/feature_upload_session?org_id={org_id}'
    upload_session_payload = {
        'file_total': 1
    }
    upload_session_resp = httpx.post(
        url=upload_session_endpoint,
        headers=headers,
        data=json.dumps(upload_session_payload)
    )
    upload_session = upload_session_resp.json()
    upload_session_id = upload_session.get('upload_session').get('id')

    # Create cloud storage location for file upload
    s3_link_endpoint = f'{RM_API}/v2/feature_upload_session/s3_link?org_id={org_id}'
    s3_link_payload = {
        "upload_session_id": upload_session_id,
        "filename": filename,
        "filesize": filesize
    }
    s3_link_resp = httpx.post(
        url=s3_link_endpoint,
        headers=headers,
        data=json.dumps(s3_link_payload)
    )
    s3_link = s3_link_resp.json()
    file_id = s3_link.get('file_id')
    s3_post = s3_link.get('post')

    # Upload file to cloud storage
    with open(filepath, 'rb') as f:
        files = {'file': (filename, f)}
        s3_post_resp = httpx.post(
            url=s3_post['url'],
            data=s3_post['fields'],
            files=files
        )
    # Status code 204 indicates a successful upload
    print(f'File upload HTTP status code: {s3_post_resp.status_code}')

    # Associate file to note
    note_files_endpoint = f'{RM_API}/solar_farms/{solar_farm_id}/equipment/{object_id}/notes/{note_id}/files?org_id={org_id}'
    note_files_payload = {
        "files": [{
            "file_id": file_id,
            "file_name": filename
        }]
    }
    note_files_resp = httpx.post(
        url=note_files_endpoint,
        headers=headers,
        data=json.dumps(note_files_payload)
    )

    if note_files_resp.status_code == 200:
        print(f'Congrats! {filename} is associated to object id {object_id}!')
    else:
        print('Error: Note files status code: {note_files_resp.status_code}')


if __name__ == '__main__':
    main()
