EXECUTE STATEMENT SET
BEGIN
-- Movement
INSERT INTO TRAIN_MOVEMENTS
WITH
    `TRAIN_MOVEMENT` AS (
        SELECT `$rowtime` AS ROWTIME, json_query(`text`, '$.*' RETURNING ARRAY<STRING>) `TEXT` from `NETWORKRAIL_TRAIN_MVT`
    ),
    `TRAIN_MOVEMENT_FROM_JSON` AS (
        SELECT
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
            CASE
                WHEN JSON_VALUE(message, '$.body.variation_status') = 'ON TIME' THEN 0
                WHEN JSON_VALUE(message, '$.body.variation_status') = 'LATE' THEN 1
                WHEN JSON_VALUE(message, '$.body.variation_status') = 'EARLY' THEN 0
                END AS late_ind,
            JSON_VALUE(message, '$.body.loc_stanox') loc_stanox,
            JSON_VALUE(message, '$.body.next_report_run_time') next_report_run_time,
            JSON_VALUE(message, '$.body.next_report_stanox') next_report_stanox,
            JSON_VALUE(message, '$.body.offroute_ind') offroute_ind,
            JSON_VALUE(message, '$.body.planned_event_type') planned_event_type,
            TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.planned_timestamp') AS BIGINT),3) planned_timestamp,
            CASE
                WHEN CHAR_LENGTH(JSON_VALUE(message,'$.body.platform')) > 0
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
            ROWTIME
        FROM `TRAIN_MOVEMENT` CROSS JOIN UNNEST(`TEXT`) AS message
        WHERE JSON_VALUE(message, '$.header.msg_type') = '0003'
    )
SELECT
    TMFJ.msg_key,
    TMFJ.msg_queue_timestamp,
    TMFJ.msg_type,
    TMFJ.original_data_source,
    TMFJ.source_system_id,
    TMFJ.actual_timestamp,
    TMFJ.auto_expected,
    TMFJ.correction_ind,
    TMFJ.delay_monitoring_point,
    TMFJ.direction_ind,
    TMFJ.division_code,
    TMFJ.event_source,
    TMFJ.event_type,
    TMFJ.gbtt_timestamp,
    TMFJ.late_ind,
    TMFJ.loc_stanox,
    L.description                                                        AS mvt_description,
    L.lat_lon                                                            AS mvt_lat_lon,
    TMFJ.next_report_run_time,
    TMFJ.next_report_stanox,
    TMFJ.offroute_ind,
    TMFJ.planned_event_type,
    TMFJ.planned_timestamp,
    TMFJ.platform,
    TMFJ.reporting_stanox,
    TMFJ.route,
    TMFJ.timetable_variation,
    TMFJ.toc_id,
    TA.toc                                                              AS toc,
    TMFJ.train_id,
    TMFJ.train_service_code,
    TMFJ.train_terminated,
    TMFJ.variation_status,
    TMFJ.variation,
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
FROM `TRAIN_MOVEMENT_FROM_JSON` TMFJ
         JOIN TRAIN_ACTIVATIONS FOR SYSTEM_TIME AS OF TMFJ.ROWTIME AS TA ON TMFJ.train_id = TA.train_id
         LEFT JOIN LOCATIONS_BY_STANOX FOR SYSTEM_TIME AS OF TMFJ.ROWTIME AS L ON TMFJ.loc_stanox = L.stanox;

-- Cancellation

INSERT INTO TRAIN_CANCELLATIONS
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

-- Reinstatement
INSERT INTO TRAIN_REINSTATEMENTS
WITH
    `TRAIN_REINSTATEMENTS` AS (
        SELECT `$rowtime` AS ROWTIME, json_query(`text`, '$.*' RETURNING ARRAY<STRING>) `TEXT` from `NETWORKRAIL_TRAIN_MVT`
    ),
    `TRAIN_REINSTATEMENTS_FROM_JSON` AS (
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
            TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.reinstatement_timestamp') AS BIGINT),3) reinstatement_timestamp,
            JSON_VALUE(message, '$.body.train_id') train_id,
            TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message, '$.body.orig_loc_timestamp') AS BIGINT),3) orig_loc_timestamp,
            JSON_VALUE(message, '$.body.canx_type') canx_type,
            ROWTIME
        FROM `TRAIN_REINSTATEMENTS` CROSS JOIN UNNEST(`TEXT`) AS message
        WHERE JSON_VALUE(message, '$.header.msg_type') = '0005'
    )
SELECT
    TRFJ.msg_queue_timestamp,
    TRFJ.msg_type,
    TRFJ.original_data_source,
    TRFJ.source_system_id,
    TRFJ.train_file_address,
    TRFJ.train_service_code,
    TRFJ.orig_loc_stanox,
    TRFJ.toc_id,
    TA.toc                                                              AS toc,
    TRFJ.dep_timestamp,
    TRFJ.loc_stanox,
    TRFJ.reinstatement_timestamp,
    TRFJ.train_id,
    TRFJ.orig_loc_timestamp,
    TRFJ.canx_type,
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
FROM `TRAIN_REINSTATEMENTS_FROM_JSON` TRFJ
         JOIN TRAIN_ACTIVATIONS FOR SYSTEM_TIME AS OF TRFJ.ROWTIME AS TA ON TRFJ.train_id = TA.train_id
         LEFT JOIN LOCATIONS_BY_STANOX FOR SYSTEM_TIME AS OF TRFJ.ROWTIME AS L ON TRFJ.loc_stanox = L.stanox;

END;