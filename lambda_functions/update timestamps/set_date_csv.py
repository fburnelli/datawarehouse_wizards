import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket = 'datawarehouse-wizards' 
    prefix = 'raw/charging_stations/'  # Original folder for source files

    # List all files in the specified bucket and prefix
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for item in response.get('Contents', []):
        key = item['Key']

        # Ignore directories and process only files
        if not key.endswith('/'):
            # Extract the date and time from the filename
            filename = key.split('/')[-1]  # Get the filename from the key
            date_str = filename.split('_')[2:5]  # Adjusted to split correctly
            date_str = '_'.join(date_str).rsplit('.', 1)[0]

            try:
                # Parse the date and time
                date_time = datetime.strptime(date_str, "%Y-%m-%d_%H-%M-%S")

                # Get the object from S3
                file_obj = s3_client.get_object(Bucket=bucket, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')

                # Read the content into a DataFrame
                df = pd.read_csv(StringIO(file_content))

                # Add the timestamp column
                df['timestamp'] = date_time

                # Convert DataFrame back to CSV
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_content = csv_buffer.getvalue()

                # Define new path in the 'processed' folder
                new_key = 'processed/charging_stations/' + filename 

                # Save the updated file to the new location
                s3_client.put_object(Bucket=bucket, Key=new_key, Body=csv_content)

                print(f"Processed and saved: {new_key}")
            
            except ValueError as e:
                # Handle cases where the filename does not match the expected pattern
                print(f"Filename pattern does not match for {filename}: {e}")

    return {
        'statusCode': 200,
        'body': "All files processed successfully"
    }
