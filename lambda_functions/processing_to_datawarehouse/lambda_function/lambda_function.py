import os
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def lambda_handler(event, context):
    #### DB 
    source_db_params = {
        "host": "warehousewizardsdatalake.c9gsisgi07br.us-east-1.rds.amazonaws.com",
        "database": "warehousewizardsdatalake",
        "user": "wizardsadmin",
        "password": "wizardshslu"
    }
    
    target_db_params = {
        "host": "warehousewizardsdatawarehouse.crw8260io7um.us-east-1.rds.amazonaws.com",
        "database": "warehousewizardsdatawarehouse",
        "user": "wizardsadmin",
        "password": "wizardshslu"
    }

    try:
        source_conn = psycopg2.connect(**source_db_params)
        source_cursor = source_conn.cursor()
        logger.info("Connected to source database")
        
        ######## Charging stations
        select_query = """
            SELECT id,
                   id AS charging_station_id,
                   CAST(SPLIT_PART(REPLACE(REPLACE(geometry_coordinates, '[', ''), ']', ''), ',', 1) AS FLOAT) AS longitude,
                   CAST(SPLIT_PART(REPLACE(REPLACE(geometry_coordinates, '[', ''), ']', ''), ',', 2) AS FLOAT) AS latitude,
                   "timestamp" AS time_id,
                   evse_status AS availability_status,
                   SUBSTRING(charging_station_names FROM 27 FOR LENGTH(charging_station_names) - 29) AS name,
                   street,
                   postal_code,
                   city,
                   parking_facility
            FROM public.charging_stations_all
        """
        source_cursor.execute(select_query)
        records = source_cursor.fetchall()
        logger.info(f"Fetched {len(records)} records from source database")
        target_conn = psycopg2.connect(**target_db_params)
        target_cursor = target_conn.cursor()

        # Insert records into the target database
        insert_query = """
            INSERT INTO public.charging_stations (
                id,
                charging_station_id,
                longitude,
                latitude,
                time_id,
                availability_status,
                name,
                street,
                postal_code,
                city,
                parking_facility
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for record in records:
            target_cursor.execute(insert_query, record)
        
        target_conn.commit()
        logger.info("Records inserted into target database")

######## TRAFFIC
        select_query = """
        WITH t AS (
  SELECT
    CAST(REPLACE(SPLIT_PART(REPLACE(REPLACE(geometry_coordinates, '[', ''), ']', ''), ',', 1), '"', '') AS FLOAT) AS longitude,
    CAST(REPLACE(SPLIT_PART(REPLACE(REPLACE(geometry_coordinates, '[', ''), ']', ''), ',', 2), '"', '') AS FLOAT) AS latitude,
    "timestamp" AS time_id,
    date_trunc('hour', "timestamp") AS truncated_time,
    jam_factor
  FROM public.trafficdata
)
SELECT 
  longitude,
  latitude,
  truncated_time AS time_id,
  AVG(jam_factor) AS jam_factor
FROM t
GROUP BY longitude, latitude, truncated_time    

        """
        source_cursor.execute(select_query)
        records = source_cursor.fetchall()
        logger.info(f"Fetched {len(records)} records from source database")
        target_conn = psycopg2.connect(**target_db_params)
        target_cursor = target_conn.cursor()

        # Insert records into the target database
        insert_query = """
            INSERT INTO public.charging_stations (
                longitude,
                latitude,
                time_id,
                jam_factor
            ) VALUES (%s, %s, %s, %s)
        """
        
        for record in records:
            target_cursor.execute(insert_query, record)
        
        target_conn.commit()
        logger.info("Records inserted into target database")


######## ACTIVITIES 
        select_query = """

        """
        source_cursor.execute(select_query)
        records = source_cursor.fetchall()
        logger.info(f"Fetched {len(records)} records from source database")
        target_conn = psycopg2.connect(**target_db_params)
        target_cursor = target_conn.cursor()

        # Insert records into the target database
        insert_query = """
            INSERT INTO public.charging_stations (
                id,
                charging_station_id,
                longitude,
                latitude,
                time_id,
                availability_status,
                name,
                street,
                postal_code,
                city,
                parking_facility
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for record in records:
            target_cursor.execute(insert_query, record)
        
        target_conn.commit()
        logger.info("Records inserted into target database")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
    
    finally:
        # Close connections
        if source_cursor:
            source_cursor.close()
        if source_conn:
            source_conn.close()
        if target_cursor:
            target_cursor.close()
        if target_conn:
            target_conn.close()

        

    return {
        "statusCode": 200,
        "body": f"Transferred {len(records)} records from source to target database"
    }
