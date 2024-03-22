import json
import requests
def lambda_handler(event, context):
    # TODO implement
    API_URL = 'http://overpass-api.de/api/interpreter'
    q="""
            [out:json]
            [bbox:47.351939,8.573595,47.361939,8.583595   ];
            nwr["amenity"];
            out;
            """
    try:
        response = requests.get( API_URL, headers = {'Accept-Charset': 'utf-8'},params  = {'data' :  q })
        data = response.text
        print(data)
    except Exception as e:
        print("An exception occurred:", e)
        data="errorone"
    return {
        'statusCode': 200,
        'body': json.dumps(data)
    }
