import requests
import os

"""
This is a simple example of a script one might use to get geojson objects from a specific farm.
In the script, we find a farm ID based on the farm name, pull an informational summary about the farm, and use that summary data to iterate through the geojson request.

You will need to provide a bearer token in order to run this script. You can find your bearer token by following the directions in the API docs found at https://docs.raptormaps.com/reference/get-api-access-token.
Additionally, you will need to know your organizational ID and the name of the farm you are interested in.
"""


bearer_token = os.environ['BEARER_TOKEN']
auth_header = {"authorization": "Bearer {}".format(bearer_token)}
base_url = 'https://api.raptormaps.com'
org_id = <YOUR_ORG_ID>

# get farm_id from /sorted_solar_farms response body by querying by name
def get_farm_id(farm_name):
	res = requests.get(base_url+'/sorted_solar_farms?org_id={}&name={}'.format(org_id, farm_name), headers=auth_header)
	print(res.json())
	farm_id = res.json()['farms'][0]['id']

	return farm_id

# Get the summary object for the farm in question
def get_farm_summary(farm_id):
	res = requests.get(base_url+'/solar_farms/{}/summary?org_id={}'.format(farm_id, org_id), headers=auth_header)
	rows = res.json()['rows']

	return rows

# For this example, we iteratively retrieve the row objects. Other object types can be retrieved in the same way by changing the object_type parameter in the request.
def get_row_objects(rows, farm_id):
	i=0
	row_data = []
	while i <= rows:
		res = requests.get(base_url+'/solar_farms/{}/objects/geojson?org_id={}&object_type=row&offset={}&limit=10'.format(farm_id, org_id, i),
			headers=auth_header)
		row_data.append(res.json())
		i = i+10

	return row_data

# This function calls the other functions in this script and prints the row data to the console. It might be more useful to dump the data to a file.
def get_row_info_by_farm_name(name):
	farm_id = get_farm_id(name)
	row_num = get_farm_summary(farm_id)
	row_data = get_row_objects(row_num, farm_id)

	print(row_data)

get_row_info_by_farm_name('<your_farm_name_here>')