select
 id charging_station_id,
 CAST(SPLIT_PART(REPLACE(REPLACE(geometry_coordinates, '[', ''), ']', ''), ',', 1) AS FLOAT) AS longitude,
 CAST(SPLIT_PART(REPLACE(REPLACE(geometry_coordinates, '[', ''), ']', ''), ',', 2) AS FLOAT) AS latitude,
 "timestamp" as time_id,
 evse_status as availability_status,
 SUBSTRING(charging_station_names FROM 27 FOR LENGTH(charging_station_names) -29) AS name,
 street,
 postal_code,
 city,
 parking_facility 
FROM public.charging_stations_all order by id,time_id
;

CREATE TABLE public.charging_stations (
	charging_station_id varchar(255) NULL,
	longitude float8 NULL,
	latitude float8 NULL,
	time_id varchar(255) NULL,
	availability_status varchar(255) NULL,
	"name" text NULL,
	street varchar(255) NULL,
	postal_code varchar(255) NULL,
	city varchar(255) NULL,
	parking_facility varchar(255) NULL
);



--------
create table public.traffic as
SELECT 
       distinct 
       lat as latitude,
       lng as longitude,
       jam_factor,
       "timestamp" as time_id 
FROM public.trafficdata_all;

CREATE TABLE public.traffic (
	latitude float8 NULL,
	longitude float8 NULL,
	jam_factor float8 NULL,
	time_id timestamp NULL
);


---------
CREATE TABLE public.activities_tmp (
	lat_ame varchar NULL,
	lat_cs varchar NULL,
	lon_ame varchar NULL,
	lon_cs varchar NULL,
	node_name varchar NULL,
	node_type varchar NULL,
	reference_date varchar NULL
);

CREATE TABLE public.activities (
	longitude float8 NULL,
	latitude float8 NULL,
	longitude_ame float8 NULL,
	latitude_ame float8 NULL,
	node_name varchar NULL,
	node_type varchar NULL,
	time_id timestamp NULL
);


INSERT INTO public.activities (longitude, latitude, latitude_ame, longitude_ame, node_name, node_type, time_id)
SELECT 
    lon_cs::FLOAT  as longitude,
    lat_cs::FLOAT  as latitude,
    lat_ame::FLOAT as latitude_ame,
    lon_ame::FLOAT as longitude_ame,
    node_name,
    node_type,
    TO_TIMESTAMP(replace(reference_date, '_', ' '), 'YYYY-MM-DD HH24-MI-SS') as time_id
FROM public.activities_tmp;
