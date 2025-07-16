-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE TRAIN_CANCELLATIONS
    WITH (
        'changelog.mode' = 'append',
        'kafka.cleanup-policy' = 'delete',
        'kafka.retention.time' = '7 days'
    )
AS
WITH
    `TRAIN_CANCELLATION` AS (
        SELECT `$rowtime` AS ROWTIME, json_query(`text`, '$.*' RETURNING ARRAY<STRING>) `TEXT` FROM `NETWORKRAIL_TRAIN_MVT`),
    `TRAIN_CANCELLATION_FROM_JSON` as (
        SELECT
            TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.header.msg_queue_timestamp') AS BIGINT),3) msg_queue_timestamp,
            JSON_VALUE(message, '$.header.msg_type') msg_type,
            JSON_VALUE(message, '$.header.original_data_source') original_data_source,
            JSON_VALUE(message, '$.header.source_system_id') source_system_id,
            JSON_VALUE(message, '$.body.train_file_address') train_file_address,
            JSON_VALUE(message, '$.body.train_service_code') train_service_code,
            JSON_VALUE(message, '$.body.orig_loc_stanox') orig_loc_stanox,
            JSON_VALUE(message, '$.body.toc_id') toc_id,
            TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.dep_timestamp') AS BIGINT),3) dep_timestamp,
            JSON_VALUE(message, '$.body.loc_stanox') loc_stanox,
            TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.canx_timestamp') AS BIGINT),3) canx_timestamp,
            JSON_VALUE(message, '$.body.canx_reason_code') canx_reason_code,
            JSON_VALUE(message, '$.body.train_id') train_id,
            TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.orig_loc_timestamp') AS BIGINT),3) orig_loc_timestamp,
            JSON_VALUE(message, '$.body.canx_type') canx_type,
            ROWTIME
        FROM `TRAIN_CANCELLATION` CROSS JOIN UNNEST(`TEXT`) AS message
        WHERE JSON_VALUE(message, '$.header.msg_type') = '0002'
    )
SELECT
    TCFJ.msg_queue_timestamp,
    TCFJ.msg_type,
    TCFJ.original_data_source,
    TCFJ.source_system_id,
    TCFJ.train_file_address,
    TCFJ.train_service_code,
    TCFJ.orig_loc_stanox,
    TCFJ.toc_id,
    TA.toc                                                              AS toc,
    TCFJ.dep_timestamp,
    TCFJ.loc_stanox,
    TCFJ.canx_timestamp,
    TCFJ.canx_reason_code,
    C.canx_reason                                                       AS canx_reason,
    TCFJ.train_id,
    TCFJ.orig_loc_timestamp,
    TCFJ.canx_type,
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
    TA.schedule_num_stops                                               AS schedule_num_stops,
    TA.train_status                                                     AS train_status,
    TA.power_type                                                       AS power_type,
    TA.seating_classes                                                  AS seating_classes,
    TA.reservations                                                     AS reservations,
    TA.sleeping_accomodation                                            AS sleeping_accomodation,
    TA.train_category                                                   AS train_category,
    TA.origin_tiploc_code                                               AS origin_tiploc_code,
    TA.origin_description                                               AS origin_description,
    TA.origin_lat_lon                                                   AS origin_lat_lon,
    TA.origin_public_departure_time                                     AS origin_public_departure_time,
    TA.origin_platform                                                  AS origin_platform,
    TA.destination_tiploc_code                                          AS destination_tiploc_code,
    TA.destination_description                                          AS destination_description,
    TA.destination_lat_lon                                              AS destination_lat_lon,
    TA.destination_public_arrival_time                                  AS destination_public_arrival_time,
    TA.destination_platform                                             AS destination_platform
FROM `TRAIN_CANCELLATION_FROM_JSON` TCFJ
  JOIN TRAIN_ACTIVATIONS FOR SYSTEM_TIME AS OF TCFJ.ROWTIME AS TA ON TCFJ.train_id = TA.train_id
  LEFT JOIN LOCATIONS_BY_STANOX FOR SYSTEM_TIME AS OF TCFJ.ROWTIME AS L ON TCFJ.loc_stanox = L.stanox
  JOIN CANCELLATION_REASON FOR SYSTEM_TIME AS OF TCFJ.ROWTIME AS C  ON TCFJ.canx_reason_code = C.canx_reason_code;
