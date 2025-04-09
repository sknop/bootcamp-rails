-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE TRAIN_REINSTATEMENTS
AS
WITH `TRAIN_MOVEMENT` AS (
    SELECT `$rowtime`, json_query(`text`, '$.*' RETURNING ARRAY<STRING>) `TEXT` from `NETWORKRAIL_TRAIN_MVT`
)
select
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.header.msg_queue_timestamp') AS BIGINT),3) msg_queue_timestamp,
    JSON_VALUE(message, '$.header.msg_type') msg_type,
    JSON_VALUE(message, '$.header.original_data_source') original_data_source,
    JSON_VALUE(message, '$.header.source_system_id') source_system_id,
    JSON_VALUE(message, '$.body.train_file_address') train_file_address,
    JSON_VALUE(message, '$.body.train_service_code') train_service_code,
    JSON_VALUE(message, '$.body.orig_loc_stanox') orig_loc_stanox,
    JSON_VALUE(message, '$.body.toc_id') toc_id,
    TA.toc                                                              AS toc,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.dep_timestamp') AS BIGINT),3) dep_timestamp,
    JSON_VALUE(message, '$.body.loc_stanox') loc_stanox,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.reinstatement_timestamp') AS BIGINT),3) reinstatement_timestamp,
    JSON_VALUE(message, '$.body.train_id') train_id,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.orig_loc_timestamp') AS BIGINT),3) orig_loc_timestamp,
    JSON_VALUE(message, '$.body.canx_type') canx_type,
    L.description                                                       AS cancellation_location,
    L.lat_lon                                                           AS cancellation_lat_lon,
    TA.schedule_source                                                  AS schedule_source,
    TA.tp_origin_timestamp                                              AS tp_origin_timestamp,
    TA.schedule_type                                                    AS schedule_type,
    TA.creation_timestamp                                               AS creation_timestamp,
    TA.origin_dep_timestamp                                             AS origin_dep_timestamp,
    TA.d1266_record_number                                              AS d1266_record_number,
    TA.train_service_code                                               AS train_service_code_02,
    TA.sched_origin_stanox                                              AS sched_origin_stanox,
    TA.train_uid                                                        AS train_uid,
    TA.train_call_mode                                                  AS train_call_mode,
    TA.tp_origin_stanox                                                 AS tp_origin_stanox,
    TA.schedule_wtt_id                                                  AS schedule_wtt_id,
    TA.train_call_type                                                  AS train_call_type,
    TA.schedule_end_date                                                AS schedule_end_date,
    coalesce(TA.schedule_key,'no_schedule_activation_found')            AS schedule_key,
    TA.sched_origin_desc                                                AS sched_origin_desc,
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
FROM `TRAIN_MOVEMENT` CROSS JOIN UNNEST(`TEXT`) AS message
                      JOIN TRAIN_ACTIVATIONS FOR SYSTEM_TIME AS OF `TRAIN_MOVEMENT`.`$rowtime` AS TA ON JSON_VALUE(message, '$.body.train_id') = TA.train_id
                      LEFT JOIN LOCATIONS L ON JSON_VALUE(message, '$.body.loc_stanox') = L.stanox
                      JOIN SCHEDULE FOR SYSTEM_TIME AS OF TA.`$rowtime` AS SCH ON TA.schedule_key = SCH.schedule_key
WHERE JSON_VALUE(message, '$.header.msg_type') = '0005';