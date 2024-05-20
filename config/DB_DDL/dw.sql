
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

CREATE TABLE public.traffic (
	latitude float8 NULL,
	longitude float8 NULL,
	jam_factor float8 NULL,
	time_id timestamp NULL
);


---------

CREATE TABLE public.activities (
	longitude float8 NULL,
	latitude float8 NULL,
	longitude_ame float8 NULL,
	latitude_ame float8 NULL,
	node_name varchar NULL,
	node_type varchar NULL,
	time_id timestamp NULL
);


---------
-- Update for 'food'
UPDATE public.activities
SET node_type = 'food'
WHERE node_type IN ('fast_food', 'canteen', 'food_court','vending_machine', 'restaurant', 'biergarten', 
                    'bar','pub', 'bbq','ice_cream', 'cafe');

-- Update for 'services'
UPDATE public.activities
SET node_type = 'services'
WHERE node_type IN ('luggage_locker', 'crematorium',  'letter_box', 'parking', 'watering_place', 
 'townhall', 'parking_space', 'clock', 'bell', 'animal_shelter', 'warehouse', 'charging_station', 
'police', 'reception_desk', 'bicycle_repair_station', 'ferry_terminal', 'shower',  
'driving_school', 'shelter', 'car_rental', 'fuel', 'atm', 'toilets', 'car_sharing', 'compressed_air',
 'nursing_home',  'coworking_space', 'check_in',  'recycling','food_sharing', 'waste_disposal',
  'courthouse', 'pharmacy', 'telephone',  'fountain', 'prison', 'vacuum_cleaner', 'photo_booth', 
  'security_control', 'sanitary_dump_station', 'water_point', 'entrance',  'doctors',
   'dog_toilet', 'post_box', 'taxi', 'loading_dock','car_wash', 'payment_centre', 'funeral_hall','bench', 'money_transfer', 'bank', 'air_vent', 'post_office',
   'dressing_room',  'clothes_dryer','fire_station');

-- Update for 'entertainment'
UPDATE public.activities
SET node_type = 'entertainment'
WHERE node_type IN ('boat_sharing', 'boat_rental',  'events_venue', 'dojo', 
                    'casino','dancing_school', 'hunting_stand', 'bicycle_wash', 'kick-scooter_parking', 'give_box', 
					'science_park', 'baking_oven', 'lost_property_office', 'greenhouse', 
					 'social_centre', 'waste_basket', 'gambling', 'traffic_park', 'arts_centre',
					  'stripclub', 'dive_centre', 'ski_rental', 'marketplace', 'conference_centre', 
					  'hammock', 'bicycle_parking', 'brothel','exhibition_centre', 'bureau_de_change', 'public_bookcase',
					   'deaddrop', 'concert_hall',  'cinema', 'public_bath',
					    'nightclub',  'theatre','locker',  'workshop',  
						'payment_centre');

-- Update for 'education'
UPDATE public.activities
SET node_type = 'education'
WHERE node_type IN ('library_dropoff', 'nursery_school','social_facility', 'childcare', 'research_institute',
                     'studio', 'school', 
                    'library', 'language_school', 'toy_library','driver_school','nursery_school', 
					'kindergarten', 'college', 'university', 
					'art_school', 'music_school' );


-- Update for 'services'
UPDATE public.activities
SET node_type = 'services'
WHERE node_type IN ('smoking_area', 'motorcycle_parking', 'grit_bin', 'drinking_water',  'mortuary',
              'printer', 'bus_station', 'parking_entrance', 'parcel_locker', 'dentist', 'infoshop',
			   'place_of_worship', 'lounger', 'ticket_validator', 'arrival');


-- Update for 'entertainment'
UPDATE public.activities
SET node_type = 'entertainment'
WHERE node_type IN ('stage', 'spa','community_centre');



-- Update for health-related services
UPDATE public.activities
SET node_type = 'services'
WHERE node_type IN ('veterinary', 'clinic', 'hospital', 'kneipp_water_cure','bicycle_rental','lockers','table');

