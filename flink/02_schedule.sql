-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE SCHEDULE (
                                schedule_key STRING,
                                train_status STRING,
                                power_type STRING,
                                seating_classes STRING,
                                reservations STRING,
                                sleeping_accomodation STRING,
                                train_category STRING,
                                origin_tiploc_code STRING,
                                origin_description STRING,
                                origin_lat_lon ROW(lat double , lon double),
                                origin_public_departure_time STRING,
                                origin_platform STRING,
                                destination_tiploc_code STRING,
                                destination_description STRING,
                                destination_lat_lon ROW(lat double , lon double),
                                destination_public_arrival_time STRING,
                                destination_platform STRING,
                                num_stops INTEGER,
                                PRIMARY KEY (schedule_key) NOT ENFORCED
)
    DISTRIBUTED BY (schedule_key) INTO 6 BUCKETS
WITH (
        'changelog.mode' = 'upsert',
        'kafka.cleanup-policy' = 'compact',
        'kafka.retention.time' = '0'
    )
AS
WITH CIF_FULL_DAILY_SCHEDULE_JSON AS (
  SELECT JSON_QUERY(CAST(val as STRING),'$.JsonScheduleV1') as json_schedule FROM `CIF_FULL_DAILY`
  WHERE
          JSON_QUERY(CAST(val as STRING),'$.JsonScheduleV1') IS NOT NULL
      AND JSON_VALUE(JSON_QUERY(CAST(val as STRING),'$.JsonScheduleV1'),'$.CIF_stp_indicator') <> 'C'
)
SELECT
    CONCAT_WS('/',
              COALESCE(JSON_VALUE(json_schedule,'$.CIF_train_uid'),''),
              COALESCE(JSON_VALUE(json_schedule,'$.schedule_start_date'),''),
              COALESCE(JSON_VALUE(json_schedule,'$.CIF_stp_indicator'),'')
    ) AS schedule_key,
    CASE
        WHEN JSON_VALUE(json_schedule,'$.train_status') = 'B' THEN 'Bus (Permanent)'
        WHEN JSON_VALUE(json_schedule,'$.train_status') = 'F' THEN 'Freight (Permanent - WTT)'
        WHEN JSON_VALUE(json_schedule,'$.train_status') = 'P' THEN 'Passenger & Parcels (Permanent - WTT)'
        WHEN JSON_VALUE(json_schedule,'$.train_status') = 'S' THEN 'Ship (Permanent)'
        WHEN JSON_VALUE(json_schedule,'$.train_status') = 'T' THEN 'Trip (Permanent)'
        WHEN JSON_VALUE(json_schedule,'$.train_status') = '1' THEN 'STP Passenger & Parcels'
        WHEN JSON_VALUE(json_schedule,'$.train_status') = '2' THEN 'STP Freight'
        WHEN JSON_VALUE(json_schedule,'$.train_status') = '3' THEN 'STP Trip'
        WHEN JSON_VALUE(json_schedule,'$.train_status') = '4' THEN 'STP Ship'
        WHEN JSON_VALUE(json_schedule,'$.train_status') = '5' THEN 'STP Bus'
        END AS train_status,
    CASE
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_power_type' returning STRING) = 'D' THEN 'Diesel'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_power_type' returning STRING) = 'DEM' THEN 'Diesel Electric Multiple Unit'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_power_type' returning STRING) = 'DMU' THEN 'Diesel Mechanical Multiple Unit'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_power_type' returning STRING) = 'E' THEN 'Electric'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_power_type' returning STRING) = 'ED' THEN 'Electro-Diesel'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_power_type' returning STRING) = 'EML' THEN 'EMU plus D, E, ED locomotive'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_power_type' returning STRING) = 'EMU' THEN 'Electric Multiple Unit'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_power_type' returning STRING) = 'HST' THEN 'High Speed Train'
        END AS power_type,
    CASE
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_class' returning STRING) = 'B' OR JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_class' returning STRING) = '' THEN 'First and standard'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_class' returning STRING) = 'S'  THEN 'Standard only'
        END AS seating_classes,
    CASE
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_reservations' returning STRING) = 'A' THEN 'Reservations compulsory'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_reservations' returning STRING) = 'E' THEN 'Reservations for bicycles essential'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_reservations' returning STRING) = 'R' THEN 'Reservations recommended'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_reservations' returning STRING) = 'S' THEN 'Reservations possible from any station'
        END AS reservations,
    CASE
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_sleepers' returning STRING) = 'B' THEN 'First and standard class'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_sleepers' returning STRING) = 'F' THEN 'First Class only'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_sleepers' returning STRING) = 'S' THEN 'Standard class only'
        END AS sleeping_accomodation,
    CASE
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'OL' THEN 'Ordinary Passenger Trains: London Underground/Metro Service'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'OU' THEN 'Ordinary Passenger Trains: Unadvertised Ordinary Passenger'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'OO' THEN 'Ordinary Passenger Trains: Ordinary Passenger'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'OS' THEN 'Ordinary Passenger Trains: Staff Train'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'OW' THEN 'Ordinary Passenger Trains: Mixed'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'XC' THEN 'Express Passenger Trains: Channel Tunnel'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'XD' THEN 'Express Passenger Trains: Sleeper (Europe Night Services)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'XI' THEN 'Express Passenger Trains: International'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'XR' THEN 'Express Passenger Trains: Motorail'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'XU' THEN 'Express Passenger Trains: Unadvertised Express'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'XX' THEN 'Express Passenger Trains: Express Passenger'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'XZ' THEN 'Express Passenger Trains: Sleeper (Domestic)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'BR' THEN 'Buses & Ships: Bus – Replacement due to engineering work'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'BS' THEN 'Buses & Ships: Bus – WTT Service'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'SS' THEN 'Buses & Ships: Ship'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'EE' THEN 'Empty Coaching Stock Trains: Empty Coaching Stock (ECS)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'EL' THEN 'Empty Coaching Stock Trains: ECS, London Underground/Metro Service'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'ES' THEN 'Empty Coaching Stock Trains: ECS & Staff'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'JJ' THEN 'Parcels and Postal Trains: Postal'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'PM' THEN 'Parcels and Postal Trains: Post Office Controlled Parcels'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'PP' THEN 'Parcels and Postal Trains: Parcels'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'PV' THEN 'Parcels and Postal Trains: Empty NPCCS'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'DD' THEN 'Departmental Trains: Departmental'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'DH' THEN 'Departmental Trains: Civil Engineer'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'DI' THEN 'Departmental Trains: Mechanical & Electrical Engineer'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'DQ' THEN 'Departmental Trains: Stores'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'DT' THEN 'Departmental Trains: Test'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'DY' THEN 'Departmental Trains: Signal & Telecommunications Engineer'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'ZB' THEN 'Light Locomotives: Locomotive & Brake Van'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'ZZ' THEN 'Light Locomotives: Light Locomotive'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'J2' THEN 'Railfreight Distribution: RfD Automotive (Components)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'H2' THEN 'Railfreight Distribution: RfD Automotive (Vehicles)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'J3' THEN 'Railfreight Distribution: RfD Edible Products (UK Contracts)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'J4' THEN 'Railfreight Distribution: RfD Industrial Minerals (UK Contracts)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'J5' THEN 'Railfreight Distribution: RfD Chemicals (UK Contracts)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'J6' THEN 'Railfreight Distribution: RfD Building Materials (UK Contracts)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'J8' THEN 'Railfreight Distribution: RfD General Merchandise (UK Contracts)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'H8' THEN 'Railfreight Distribution: RfD European'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'J9' THEN 'Railfreight Distribution: RfD Freightliner (Contracts)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'H9' THEN 'Railfreight Distribution: RfD Freightliner (Other)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'A0' THEN 'Trainload Freight: Coal (Distributive)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'E0' THEN 'Trainload Freight: Coal (Electricity) MGR'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'B0' THEN 'Trainload Freight: Coal (Other) and Nuclear'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'B1' THEN 'Trainload Freight: Metals'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'B4' THEN 'Trainload Freight: Aggregates'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'B5' THEN 'Trainload Freight: Domestic and Industrial Waste'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'B6' THEN 'Trainload Freight: Building Materials (TLF)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'B7' THEN 'Trainload Freight: Petroleum Products'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'H0' THEN 'Railfreight Distribution (Channel Tunnel): RfD European Channel Tunnel (Mixed Business)'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'H1' THEN 'Railfreight Distribution (Channel Tunnel): RfD European Channel Tunnel Intermodal'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'H3' THEN 'Railfreight Distribution (Channel Tunnel): RfD European Channel Tunnel Automotive'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'H4' THEN 'Railfreight Distribution (Channel Tunnel): RfD European Channel Tunnel Contract Services'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'H5' THEN 'Railfreight Distribution (Channel Tunnel): RfD European Channel Tunnel Haulmark'
        WHEN JSON_VALUE(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.CIF_train_category' returning STRING) =  'H6' THEN 'Railfreight Distribution (Channel Tunnel): RfD European Channel Tunnel Joint Venture'
        END AS train_category,
    JSON_VALUE(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>)[1],'$.tiploc_code') as origin_tiploc_code,
    T_SRC.description AS origin_description,
    T_SRC.lat_lon AS origin_lat_lon,
    JSON_VALUE(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>)[1],'$.public_departure') as origin_public_departure_time,
    JSON_VALUE(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>)[1],'$.platform') as origin_platform,
    JSON_VALUE(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>)[CARDINALITY(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>))],'$.tiploc_code') as destination_tiploc_code,
    T_DST.description AS destination_description,
    T_DST.lat_lon AS destination_lat_lon,
    JSON_VALUE(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>)[CARDINALITY(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>))],'$.public_arrival') as destination_public_arrival_time,
    JSON_VALUE(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>)[CARDINALITY(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>))],'$.platform') as destination_platform,
    CARDINALITY(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>)) AS num_stops
FROM CIF_FULL_DAILY_SCHEDULE_JSON
         LEFT JOIN LOCATIONS T_SRC
                   ON JSON_VALUE(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>)[1],'$.tiploc_code') = T_SRC.tiploc
         LEFT JOIN LOCATIONS T_DST
                   ON JSON_VALUE(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>)[CARDINALITY(JSON_QUERY(JSON_QUERY(json_schedule,'$.schedule_segment'),'$.schedule_location' returning array<string>))],'$.tiploc_code') = T_DST.tiploc
;
