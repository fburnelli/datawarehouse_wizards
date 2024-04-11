import json
import boto3
import requests
import pandas as pd
from datetime import datetime
from io import StringIO


def load_data_from_s3(bucket, key):
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    data = response['Body'].read().decode('utf-8')
    data_frame = pd.read_csv(StringIO(data))
    return data_frame

def get_real_time_flow_info(latitude, longitude, radius, apiKey, url):
    params = {
        "apiKey": apiKey,
        "in": f"circle:{latitude},{longitude};r={radius}",
        "locationReferencing": "olr,shape",
        "minJamFactor": 0,
        "maxJamFactor": 10,
        "functionalClasses": "1,2,3,4,5",
        "useRefReplacements": True
    }
    response = requests.get(url, params=params)
    if response.ok:
        return response.json()
    else:
        response.raise_for_status()

def lambda_handler(event, context):
    apiKey = ""
    url = "https://data.traffic.hereapi.com/v7/flow"
    radius = 10  # radius in meters

    bucket_name_2 = 'dwl-chargingstation-avaiability'
    key = 'geo-coordinates-chargingstations-final.csv'
    try:
        data = load_data_from_s3(bucket_name_2, key)
        # Weiter mit der Verarbeitung von `data`
        print("Daten erfolgreich geladen und DataFrame erstellt:", data.head())
    except Exception as e:
        print(f"Fehler beim Laden der Daten: {e}")
        return {
            'statusCode': 500,
            'body': f'Fehler beim Laden der Daten: {e}'
        }
    
    flattened_data = []
    for index, row in data.iterrows():
        try:
            traffic_info = get_real_time_flow_info(row['Latitude'], row['Longitude'], radius, apiKey, url)
            for result in traffic_info['results']:
                for link in result['location']['shape']['links']:
                    for point in link['points']:
                        flattened_data.append({
                            'geometry_coordinates': f"[{row['Longitude']}, {row['Latitude']}]",
                            'description': result['location']['description'],
                            'location_length': result['location']['length'],
                            'lat': point['lat'],
                            'lng': point['lng'],
                            'link_length': link['length'],
                            'speed': result['currentFlow']['speed'],
                            'freeFlow': result['currentFlow']['freeFlow'],
                            'jamFactor': result['currentFlow']['jamFactor']
                        })
        except Exception as e:
            print(f"Error fetching traffic data for location {index}: {e}")

    # Save to S3
    s3 = boto3.client('s3')
    bucket_name_1 = 'datawarehouse-wizards'
    folder_name = 'raw/traffic/'
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Format: YYYY-MM-DD_hh-mm-ss
    object_key_1 = f"{folder_name}traffic_data_{current_time}.json"  # Filename includes current date and time
    object_key_2 = f"{folder_name}traffic_data_{current_time}.json"
    s3.put_object(Bucket=bucket_name_1, Key=object_key_1, Body=json.dumps(flattened_data), ACL="bucket-owner-full-control")
    s3.put_object(Bucket=bucket_name_2, Key=object_key_2, Body=json.dumps(flattened_data), ACL="bucket-owner-full-control")


    return {
        'statusCode': 200,
        'body': f'Data successfully saved to S3 with filename {object_key_1}.'
    }
