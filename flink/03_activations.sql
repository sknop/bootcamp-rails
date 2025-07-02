-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE TRAIN_ACTIVATIONS (
        train_id				STRING,
        schedule_key      		STRING,
        msg_queue_timestamp 	TIMESTAMP_LTZ,
        msg_type				STRING,
        original_data_source	STRING,
        source_system_id		STRING,
        tp_origin_timestamp	    DATE,
        schedule_type			STRING,
        creation_timestamp		TIMESTAMP_LTZ,
        origin_dep_timestamp	TIMESTAMP_LTZ,
        toc_id					STRING,
        toc					    STRING,
        d1266_record_number	    STRING,
        train_service_code		STRING,
        sched_origin_stanox	    STRING,
        train_uid				STRING,
        train_call_mode		    STRING,
        schedule_start_date	    DATE,
        tp_origin_stanox		STRING,
        schedule_wtt_id		    STRING,
        train_call_type		    STRING,
        schedule_end_date		DATE,
        sched_origin_desc		STRING,
        lat_lon				    ROW(lat double , lon double),
        schedule_num_stops      INTEGER,
        train_status            STRING,
        power_type              STRING,
        seating_classes         STRING,
        reservations            STRING,
        sleeping_accomodation   STRING,
        train_category          STRING,
        origin_tiploc_code      STRING,
        origin_description      STRING,
        origin_lat_lon          ROW(lat double , lon double),
        origin_public_departure_time    STRING,
        origin_platform                 STRING,
        destination_tiploc_code         STRING,
        destination_description         STRING,
        destination_lat_lon             ROW(lat double , lon double),
        destination_public_arrival_time STRING,
        destination_platform            STRING,
        PRIMARY KEY (train_id) NOT ENFORCED
)
    WITH (
        'changelog.mode' = 'upsert',
        'kafka.cleanup-policy' = 'compact',
        'kafka.retention.time' = '31 days'
        )
AS
    WITH `TRAIN_MOVEMENT` AS (
        SELECT json_query(`text`, '$.*' RETURNING ARRAY<STRING>) `TEXT` from `NETWORKRAIL_TRAIN_MVT`
    )
SELECT /*+ STATE_TTL('SCH'='1d') */
    COALESCE(JSON_VALUE(message, '$.body.train_id'),'No ID') train_id,
    CONCAT_WS('/',
              COALESCE(JSON_VALUE(message, '$.body.train_uid'),''),
              COALESCE(JSON_VALUE(message, '$.body.schedule_start_date'),''),
              COALESCE(CASE WHEN JSON_VALUE(message, '$.body.schedule_type') = 'O' THEN 'P'
                            WHEN JSON_VALUE(message, '$.body.schedule_type') = 'P' THEN 'O'
                            ELSE JSON_VALUE(message, '$.body.schedule_type')
                           END,'')
    ) AS schedule_key,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.header.msg_queue_timestamp') AS BIGINT),3) msg_queue_timestamp,
    JSON_VALUE(message, '$.header.msg_type') msg_type,
    JSON_VALUE(message, '$.header.original_data_source') original_data_source,
    JSON_VALUE(message, '$.header.source_system_id') source_system_id,
    JSON_VALUE(message, '$.body.schedule_source') schedule_source,
    TO_DATE(JSON_VALUE(message, '$.body.tp_origin_timestamp')) tp_origin_timestamp,
    CASE WHEN JSON_VALUE(message, '$.body.schedule_source') = 'O' THEN 'P'
         WHEN JSON_VALUE(message, '$.body.schedule_source') = 'P' THEN 'O'
         ELSE JSON_VALUE(message, '$.body.schedule_source')
    END AS schedule_type,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.creation_timestamp') AS BIGINT),3) creation_timestamp,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.origin_dep_timestamp') AS BIGINT),3) origin_dep_timestamp,
    JSON_VALUE(message, '$.body.toc_id') toc_id,
    COALESCE(TC.company_name , 'Unknown TOC_ID')  AS toc,
    JSON_VALUE(message, '$.body.d1266_record_number') d1266_record_number,
    JSON_VALUE(message, '$.body.train_service_code') train_service_code,
    JSON_VALUE(message, '$.body.sched_origin_stanox') sched_origin_stanox,
    JSON_VALUE(message, '$.body.train_uid') train_uid,
    JSON_VALUE(message, '$.body.train_call_mode') train_call_mode,
    TO_DATE(JSON_VALUE(message, '$.body.schedule_start_date')) schedule_start_date,
    JSON_VALUE(message, '$.body.tp_origin_stanox') tp_origin_stanox,
    JSON_VALUE(message, '$.body.schedule_wtt_id') schedule_wtt_id,
    JSON_VALUE(message, '$.body.train_call_type') train_call_type,
    TO_DATE(JSON_VALUE(message, '$.body.schedule_end_date')) schedule_end_date,
    L.description as sched_origin_desc,
    L.lat_lon as lat_lon,
    SCH.num_stops                                                       AS schedule_num_stops,
    SCH.train_status                                                    AS train_status,
    SCH.power_type                                                      AS power_type,
    SCH.seating_classes                                                 AS seating_classes,
    SCH.reservations                                                    AS reservations,
    SCH.sleeping_accomodation                                           AS sleeping_accomodation,
    SCH.train_category                                                  AS train_category,
    SCH.origin_tiploc_code                                              AS origin_tiploc_code,
    SCH.origin_description                                              AS origin_description,
    SCH.origin_lat_lon                                                  AS origin_lat_lon,
    SCH.origin_public_departure_time                                    AS origin_public_departure_time,
    SCH.origin_platform                                                 AS origin_platform,
    SCH.destination_tiploc_code                                         AS destination_tiploc_code,
    SCH.destination_description                                         AS destination_description,
    SCH.destination_lat_lon                                             AS destination_lat_lon,
    SCH.destination_public_arrival_time                                 AS destination_public_arrival_time,
    SCH.destination_platform                                            AS destination_platform
from `TRAIN_MOVEMENT` CROSS JOIN UNNEST(`TEXT`) AS message
                      LEFT JOIN LOCATIONS_BY_STANOX L ON JSON_VALUE(message, '$.body.sched_origin_stanox') = L.stanox
                      LEFT JOIN TOC_CODES TC ON JSON_VALUE(message, '$.body.toc_id') = TC.toc_id
                      JOIN SCHEDULE AS SCH ON CONCAT_WS('/',
                                                        COALESCE(JSON_VALUE(message, '$.body.train_uid'),''),
                                                        COALESCE(JSON_VALUE(message, '$.body.schedule_start_date'),''),
                                                        COALESCE(CASE WHEN JSON_VALUE(message, '$.body.schedule_type') = 'O' THEN 'P'
                                                                      WHEN JSON_VALUE(message, '$.body.schedule_type') = 'P' THEN 'O'
                                                                      ELSE JSON_VALUE(message, '$.body.schedule_type')
                                                                     END,'')
                                              ) = SCH.schedule_key
WHERE JSON_VALUE(message, '$.header.msg_type') = '0001';
