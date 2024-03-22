import requests 
import json
import csv
#from geopy.distance import geodesic
#from shapely.geometry import Polygon
from datetime import datetime

API_URL = 'http://overpass-api.de/api/interpreter'
CURRENT_DATE = datetime.now().strftime("%Y%m%d")
#import overpass
#api = overpass.API()
#response = api.get('node["addr:city"="Zürich"]')
# pip install geopy

def create_proximity_bbox(center_lat, center_lon,delta = 0.01):
    # Center coordinates
    #center_lat, center_lon = 47.361939, 8.583595
    # Distance in meters for the perimeter
    # perimeter_distance = 500

    # Calculate the distance in latitude and longitude
    #perimeter_point = geodesic(kilometers=perimeter_distance / 1000).destination((center_lat, center_lon), 0)
    #perimeter_lat = perimeter_point.latitude
    #perimeter_lon = perimeter_point.longitude


    
    # Calculate the bounding box coordinates
    #min_lat = center_lat - (perimeter_lat - center_lat)
    #max_lat = center_lat + (perimeter_lat - center_lat)
    #min_lon = center_lon - (perimeter_lon - center_lon)
    #max_lon = center_lon + (perimeter_lon - center_lon)


    min_lat = center_lat + delta
    max_lat = center_lat - delta
    min_lon = center_lon - delta
    max_lon = center_lon + delta


    # Print the bounding box coordinates
    bbox=  f"{max_lat},{min_lon},{min_lat},{max_lon}"
    #print(bbox)
    return bbox

def persist_data(data):
    filename = f"{CURRENT_DATE}_extracted_activities.json"

    with open(filename, "w") as json_file:
        json.dump(data, json_file, indent=4)


#q=f"""
#[out:json]
#[bbox:  {create_proximity_bbox(47.361939, 8.583595)} ];
#nwr["amenity"];
#out;
#"""

#47.351939,8.573595,47.361939,8.583595

def extract_fields():
    pass
def process_charging_stations(csv_file):
    with open(csv_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        results = []
        for row in reader:
            geometry_coordinates = row.get('geometry_coordinates', '').strip("[]").split(",")
            center_lat = float(geometry_coordinates[0])
            center_lon = float(geometry_coordinates[1])
            station_id = row.get('id', '')
            q=f"""
            [out:json]
            [bbox:  {create_proximity_bbox(center_lon, center_lat)} ];
            nwr["amenity"];
            out;
            """
            
            charging_station = {"center_lat":center_lat,"center_lon":center_lon,"station_id":station_id}
            response = requests.get( API_URL, headers = {'Accept-Charset': 'utf-8'},params  = {'data'          :  q })

            data = response.text

            for element in json.loads(data).get("elements"):                
                if element.get("tags",None) is not None:
                    if element.get("tags").get("amenity") is not None:
                        charging_station['node_type'] = element.get("tags").get("amenity")
                        charging_station['node_name'] = element.get("tags").get("name")
                        results.append(charging_station.copy())
                if element.get("tags").get("leisure") is not None:
                    charging_station['node_type'] = element.get("tags").get("leisure")
                    charging_station['node_name'] = element.get("tags").get("name")
                    results.append(charging_station.copy())
                    #print(f'leisure:{element.get("tags").get("leisure")} {element.get("tags").get("addr:street")} {element.get("tags").get("name")}')
                if element.get("tags").get("shop") is not None:
                    charging_station['node_type'] = element.get("tags").get("leisure")
                    charging_station['node_name'] = element.get("tags").get("shop") 
                    results.append(charging_station.copy())

            fields = ['center_lat','center_lon','station_id','node_type','node_name'] 
            #fieldnames = list(data[0].keys())      
            with open(f"{CURRENT_DATE}_activities.csv", mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=fields)
                writer.writeheader()
                for row in results:
                   writer.writerow(row)
            

def process():
    # specific charging spot
    #create_proximity_bbox(47.361939, 8.583595)
    #create_proximity_bbox(47.361939, 8.583595)
    
    #get all Zuerich amenity data
    process_charging_stations('../../data/240322_EV-CharingStations-active.csv')
    1/0 
    input("run FULL>>")
    q_ZH="""
    [out:json]
    [bbox:  47.27,8.36,47.47,8.75];
    nwr["amenity"];
    out;
    """

    response = requests.get(
   'http://overpass-api.de/api/interpreter',
    headers = {'Accept-Charset': 'utf-8'},
    params  = {'data'          :  q_ZH }
    )

    data = response.text
    #print(data)
    persist_data(data)

    for element in json.loads(data).get("elements"):
        if False and element.get("tags",None) is not None:
            if element.get("tags").get("amenity") is not None:
                print(f'amenity:{element.get("tags").get("amenity")} street:{element.get("tags").get("addr:street")} name:{element.get("tags").get("name")}')
                print(element.get("nodes"))
            if element.get("tags").get("leisure") is not None:
                print(f'leisure:{element.get("tags").get("leisure")} {element.get("tags").get("addr:street")} {element.get("tags").get("name")}')
            if element.get("tags").get("shop") is not None:
                print(f'shop:{element.get("tags").get("shop")} {element.get("tags").get("addr:street")} {element.get("tags").get("name")}')

    
    

if __name__ == "__main__":
    process()
