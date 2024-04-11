import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

# Initialisieren Sie den S3-Client
s3 = boto3.client('s3')

# Base URL für den API-Aufruf
url = "http://ich-tanke-strom.switzerlandnorth.cloudapp.azure.com:8080/geoserver/ich-tanke-strom/ows"

# Parameter für den API-Aufruf
params = {
    "service": "WFS",
    "version": "1.0.0",
    "request": "GetFeature",
    "typeName": "ich-tanke-strom:evse",
    "maxFeatures": "1000",
    "outputFormat": "application/json",
    # Hinweis: CQL-Filter wird hier weggelassen, um alle Datensätze abzurufen
    "cql_filter": "Address.City ilike '%Zürich%' AND Accessibility like 'Free publicly accessible'"
}

def lambda_handler(event, context):
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data['features'], sep='_')
        
        # Konvertieren Sie den DataFrame in einen CSV-String
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        
        folder = "raw/charging_stations/"
        filename = f"{folder}evse_data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
        
        # Hochladen der Daten in den S3-Bucket
        s3.put_object(Bucket='datawarehouse-wizards', Key=filename, Body=csv_buffer.getvalue(), ACL="bucket-owner-full-control")
        
        s3.put_object(Bucket='dwl-chargingstation-avaiability', Key=filename, Body=csv_buffer.getvalue())

        
        return {
            'statusCode': 200,
            'body': f"Successfully uploaded {filename} to bucket"
        }
    else:
        return {
            'statusCode': response.status_code,
            'body': "Failed to fetch data"
        }
