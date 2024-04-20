import requests 
import json
import csv
from datetime import datetime
import boto3
import io

API_URL = 'http://overpass-api.de/api/interpreter'
REFERENCE_DATE = '2024-03-23_13-12-40'
REFERENCE_FILE = f"evse_data_{REFERENCE_DATE}.csv"
bucket_name = 'datawarehouse-wizards'
file_key = f'raw/charging_stations/evse_data_{REFERENCE_DATE}.csv'



s3 = boto3.client('s3')

def create_proximity_bbox(center_lat, center_lon,delta = 0.01):
    # Center coordinates
    #center_lat, center_lon = 47.361939, 8.583595
    # Distance in meters for the perimeter
    # perimeter_distance = 500
    
    # Calculate the bounding box coordinates
    min_lat = center_lat + delta
    max_lat = center_lat - delta
    min_lon = center_lon - delta
    max_lon = center_lon + delta

    # Print the bounding box coordinates
    bbox=  f"{max_lat},{min_lon},{min_lat},{max_lon}"
    return bbox

            
def get_rows(center_lon,center_lat):
    """ generste row for data lake """
    rows = []
    q=f"""
        [out:json]
        [bbox:  {create_proximity_bbox(center_lon, center_lat)} ];
        nwr["amenity"];
        out;
        """
    response = requests.get( API_URL, headers = {'Accept-Charset': 'utf-8'},params  = {'data'          :  q })
    data = response.text

    for element in json.loads(data).get("elements"):     
        row = {"lon_cs":center_lat,
               "lat_cs":center_lon,
               "lon_ame":element.get("lon"),
               "lat_ame":element.get("lat"),
               "reference_date" : REFERENCE_DATE               
               }
        if element.get("tags",None) is not None:
            if element.get("tags").get("amenity") is not None:
                row['node_type'] = element.get("tags").get("amenity",'').replace(',',' ').replace(';',' ')
                row['node_name'] = element.get("tags").get("name",'').replace(',',' ').replace(';',' ')
                rows.append(row.copy())
            if element.get("tags").get("leisure") is not None:
                row['node_type'] = element.get("tags").get("leisure",'').replace(',',' ').replace(';',' ')
                row['node_name'] = element.get("tags").get("name",'').replace(',',' ').replace(';',' ')
            if element.get("tags").get("shop") is not None:
                row['node_type'] = element.get("tags").get("leisure",'').replace(',',' ').replace(';',' ')
                row['node_name'] = element.get("tags").get("shop",'').replace(',',' ').replace(';',' ')
    
    unique_tuples = {tuple(sorted(d.items())) for d in rows}
    rows = [dict(item) for item in unique_tuples]
    return rows

def process_charging_stations(file_key):
    rows = []
    #generate unique coordinates for from charging stations
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_data = response['Body'].read().decode('utf-8')
        
    reader = csv.DictReader(io.StringIO(csv_data))
    unique_coordinates_cs = []
    unique_coordinates = set()  
    for row in reader:
        geometry_coordinates = row.get('geometry_coordinates', '').strip("[]").split(",")
        center_lat = float(geometry_coordinates[1])
        center_lon = float(geometry_coordinates[0])

        coordinates = (center_lon, center_lat)
        if coordinates not in unique_coordinates:
            unique_coordinates.add(coordinates) 
            unique_coordinates_cs.append({"longitude": center_lon, "latitude": center_lat})
    print("unique charging stations coordinates:",len(unique_coordinates_cs))
   
    
    # for each coordinate get amenities in 1km kreis
    for coordinate in unique_coordinates:
        lat = coordinate[1]
        lon = coordinate[0]
        rows.extend(get_rows(lat,lon))
        print("amenities collected",len(rows))

    unique_tuples = {tuple(sorted(d.items())) for d in rows}
    rows = [dict(item) for item in unique_tuples]
    print("amenities collected DISTINCT",len(rows))
        
    fieldnames = list(rows[0].keys())      
    with open(f"activities_{REFERENCE_DATE}.lambda.csv", mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)       

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    
    # Write CSV data to S3
    file_key = f'raw/activities/activities_{REFERENCE_DATE}.lambda.csv'
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue(),
            ContentType='text/csv'
        )
        return {
            'statusCode': 200,
            'body': f'File written to S3://{bucket_name}/{file_key}'
        }
    except Exception as e:
        print(f"Error writing file to S3: {e}")
        return {
            'statusCode': 500,
            'body': 'Error writing file to S3'
        }


def lambda_handler(event, context):
    #get all Zuerich amenity data around the charging stations
    process_charging_stations(file_key)
    print("...END")
