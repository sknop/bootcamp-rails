-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE FLINK_TRAIN_MOVEMENTS
AS
WITH `TRAIN_MOVEMENT` AS (
    SELECT json_query(`text`, '$.*' RETURNING ARRAY<STRING>) `TEXT` from `NETWORKRAIL_TRAIN_MVT`
)
select
    CONCAT_WS('/',
              COALESCE(JSON_VALUE(message, '$.body.train_id'),''),
              COALESCE(JSON_VALUE(message, '$.body.planned_event_type'),''),
              COALESCE(JSON_VALUE(message, '$.body.loc_stanox'),'')
    ) AS msg_key,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.header.msg_queue_timestamp') AS BIGINT),3) msg_queue_timestamp,
    JSON_VALUE(message, '$.header.msg_type') msg_type,
    JSON_VALUE(message, '$.header.original_data_source') original_data_source,
    JSON_VALUE(message, '$.header.source_system_id') source_system_id,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.actual_timestamp') AS BIGINT),3) actual_timestamp,
    JSON_VALUE(message, '$.body.auto_expected') auto_expected,
    JSON_VALUE(message, '$.body.correction_ind') correction_ind,
    JSON_VALUE(message, '$.body.delay_monitoring_point') delay_monitoring_point,
    JSON_VALUE(message, '$.body.direction_ind') direction_ind,
    JSON_VALUE(message, '$.body.division_code') division_code,
    JSON_VALUE(message, '$.body.event_source') event_source,
    JSON_VALUE(message, '$.body.event_type') event_type,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.gbtt_timestamp') AS BIGINT),3) gbtt_timestamp,
    CASE WHEN JSON_VALUE(message, '$.body.variation_status') = 'ON TIME' THEN 0
         WHEN JSON_VALUE(message, '$.body.variation_status') = 'LATE' THEN 1
         WHEN JSON_VALUE(message, '$.body.variation_status') = 'EARLY' THEN 0
        END AS late_ind,
    JSON_VALUE(message, '$.body.loc_stanox') loc_stanox,
    L.description                                                       AS mvt_description,
    L.lat_lon                                                           AS mvt_lat_lon,
    JSON_VALUE(message, '$.body.next_report_run_time') next_report_run_time,
    JSON_VALUE(message, '$.body.next_report_stanox') next_report_stanox,
    JSON_VALUE(message, '$.body.offroute_ind') offroute_ind,
    JSON_VALUE(message, '$.body.planned_event_type') planned_event_type,
    TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.planned_timestamp') AS BIGINT),3) planned_timestamp,
    CASE WHEN CHAR_LENGTH(JSON_VALUE(message,'$.body.platform')) > 0
             THEN CONCAT('Platform ', JSON_VALUE(message,'$.body.platform'))
         ELSE ''
        END AS platform,
    JSON_VALUE(message, '$.body.reporting_stanox') reporting_stanox,
    JSON_VALUE(message, '$.body.route') route,
    JSON_VALUE(message, '$.body.timetable_variation') timetable_variation,
    JSON_VALUE(message, '$.body.toc_id') toc_id,
    JSON_VALUE(message, '$.body.train_id') train_id,
    JSON_VALUE(message, '$.body.train_service_code') train_service_code,
    JSON_VALUE(message, '$.body.train_terminated') train_terminated,
    JSON_VALUE(message, '$.body.variation_status') variation_status,
    CASE WHEN JSON_VALUE(message, '$.body.variation_status') = 'ON TIME' THEN 'ON TIME'
         WHEN JSON_VALUE(message, '$.body.variation_status') = 'LATE' THEN CONCAT(JSON_VALUE(message, '$.body.timetable_variation'), ' MINS LATE')
         WHEN JSON_VALUE(message, '$.body.variation_status') = 'EARLY' THEN CONCAT(JSON_VALUE(message, '$.body.timetable_variation'), ' MINS EARLY')
        END AS variation,
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
                      JOIN FLINK_TRAIN_ACTIVATIONS TA ON JSON_VALUE(message, '$.body.train_id') = TA.train_id
                      LEFT JOIN FLINK_LOCATIONS L ON JSON_VALUE(message, '$.body.loc_stanox') = L.stanox
                      JOIN FLINK_SCHEDULE SCH ON TA.schedule_key = SCH.schedule_key
WHERE JSON_VALUE(message, '$.header.msg_type') = '0003';