import boto3
import requests
import json
import time
import base64
import uuid
from googleplaces import GooglePlaces, types


# Step 1: Set up your environment
AWS_ACCESS_KEY_ID = 'access_key'
AWS_SECRET_ACCESS_KEY = 'secret_access_key'
REGION_NAME = 'region_name'
API_KEY = 'google_api_key'


def get_restaurant_data():
    google_places = GooglePlaces(API_KEY)
    query_result = google_places.nearby_search(
        location='Paris', keyword='Restaurants',
        radius=1000, types=[types.TYPE_RESTAURANT])

    places = []
    for place in query_result.places:
        place.get_details()
        places.append({"name": place.name,
                       "address": place.formatted_address,
                       "local_phone_number": place.local_phone_number,
                       "international_phone_number": place.international_phone_number})
    return places

def send_data_to_kinesis(kinesis_client, stream_name, restaurants):
    for restaurant in restaurants:
        partition_key = restaurant['name']
        data = json.dumps(restaurant) + '\n'
        encoded_data = data.encode('utf-8')
        kinesis_client.put_record(StreamName=stream_name, Data=encoded_data, PartitionKey=partition_key)

if __name__ == '__main__':
    kinesis_client = boto3.client('kinesis', region_name=REGION_NAME, aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    stream_name = 'stream_name'  # Replace with your Kinesis Stream name

    restaurants = get_restaurant_data()
    send_data_to_kinesis(kinesis_client, stream_name, restaurants)
