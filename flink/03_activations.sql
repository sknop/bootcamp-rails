-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE FLINK_TRAIN_ACTIVATIONS (
                                         train_id				STRING,
                                         schedule_key      		STRING,
                                         msg_queue_timestamp 	TIMESTAMP_LTZ,
                                         msg_type				STRING,
                                         original_data_source	STRING,
                                         source_system_id		STRING,
                                         tp_origin_timestamp	DATE,
                                         schedule_type			STRING,
                                         creation_timestamp		TIMESTAMP_LTZ,
                                         origin_dep_timestamp	TIMESTAMP_LTZ,
                                         toc_id					STRING,
                                         toc					STRING,
                                         d1266_record_number	STRING,
                                         train_service_code		STRING,
                                         sched_origin_stanox	STRING,
                                         train_uid				STRING,
                                         train_call_mode		STRING,
                                         schedule_start_date	DATE,
                                         tp_origin_stanox		STRING,
                                         schedule_wtt_id		STRING,
                                         train_call_type		STRING,
                                         schedule_end_date		DATE,
                                         sched_origin_desc		STRING,
                                         lat_lon				ROW(lat double , lon double),
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
SELECT
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
    L.lat_lon as lat_lon
from `TRAIN_MOVEMENT` CROSS JOIN UNNEST(`TEXT`) AS message
                      LEFT JOIN FLINK_LOCATIONS L ON JSON_VALUE(message, '$.body.sched_origin_stanox') = L.stanox
                      LEFT JOIN TOC_CODES TC ON JSON_VALUE(message, '$.body.toc_id') = TC.toc_id
WHERE JSON_VALUE(message, '$.header.msg_type') = '0001';
