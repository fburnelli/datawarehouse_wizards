import boto3
import json
from io import StringIO
from datetime import datetime

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket = 'datawarehouse-wizards' 
    prefix = 'raw/traffic/'

    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for item in response.get('Contents', []):
        key = item['Key']
        if not key.endswith('/'):
            filename = key.split('/')[-1]
            date_str = '_'.join(filename.split('_')[2:5]).rsplit('.', 1)[0]

            try:
                date_time = datetime.strptime(date_str, "%Y-%m-%d_%H-%M-%S")
                file_obj = s3_client.get_object(Bucket=bucket, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')
                data = json.loads(file_content)

                if isinstance(data, list):
                    # Add timestamp to each dictionary in the list
                    for item in data:
                        item['timestamp'] = date_time.isoformat()

                json_content = json.dumps(data)
                new_key = 'processed/traffic/' + filename 
                s3_client.put_object(Bucket=bucket, Key=new_key, Body=json_content)
                print(f"Processed and saved: {new_key}")

            except ValueError as e:
                print(f"Filename pattern does not match for {filename}: {e}")

    return {
        'statusCode': 200,
        'body': "All files processed successfully"
    }