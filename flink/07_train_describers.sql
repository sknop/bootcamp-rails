-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE TRAIN_DESCRIBERS
AS
WITH TD AS (
    select json_query(`text`, '$.*' RETURNING ARRAY<STRING>) `TEXT` from `TD_ALL_SIG_AREA`
)
SELECT
    CASE
        WHEN JSON_EXISTS(message,'$.CA_MSG') THEN JSON_VALUE(message,'$.CA_MSG.msg_type')
        WHEN JSON_EXISTS(message,'$.CB_MSG') THEN JSON_VALUE(message,'$.CB_MSG.msg_type')
        WHEN JSON_EXISTS(message,'$.CC_MSG') THEN JSON_VALUE(message,'$.CC_MSG.msg_type')
        WHEN JSON_EXISTS(message,'$.CT_MSG') THEN JSON_VALUE(message,'$.CT_MSG.msg_type')
        WHEN JSON_EXISTS(message,'$.SF_MSG') THEN JSON_VALUE(message,'$.SF_MSG.msg_type')
        WHEN JSON_EXISTS(message,'$.SG_MSG') THEN JSON_VALUE(message,'$.SG_MSG.msg_type')
        WHEN JSON_EXISTS(message,'$.SH_MSG') THEN JSON_VALUE(message,'$.CH_MSG.msg_type')
        ELSE 'UNKNOWN MESSAGE TYPE'
        END AS msg_type,
    CASE
        WHEN JSON_EXISTS(message,'$.CA_MSG') THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message,'$.CA_MSG.time') AS BIGINT),3)
        WHEN JSON_EXISTS(message,'$.CB_MSG') THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message,'$.CB_MSG.time') AS BIGINT),3)
        WHEN JSON_EXISTS(message,'$.CC_MSG') THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message,'$.CC_MSG.time') AS BIGINT),3)
        WHEN JSON_EXISTS(message,'$.CT_MSG') THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message,'$.CT_MSG.time') AS BIGINT),3)
        WHEN JSON_EXISTS(message,'$.SF_MSG') THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message,'$.SF_MSG.time') AS BIGINT),3)
        WHEN JSON_EXISTS(message,'$.SG_MSG') THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message,'$.SG_MSG.time') AS BIGINT),3)
        WHEN JSON_EXISTS(message,'$.SH_MSG') THEN TO_TIMESTAMP_LTZ(CAST(JSON_VALUE(message,'$.CH_MSG.time') AS BIGINT),3)
        END AS `time`,
    CASE
        WHEN JSON_EXISTS(message,'$.CA_MSG') THEN JSON_VALUE(message,'$.CA_MSG.area_id')
        WHEN JSON_EXISTS(message,'$.CB_MSG') THEN JSON_VALUE(message,'$.CB_MSG.area_id')
        WHEN JSON_EXISTS(message,'$.CC_MSG') THEN JSON_VALUE(message,'$.CC_MSG.area_id')
        WHEN JSON_EXISTS(message,'$.CT_MSG') THEN JSON_VALUE(message,'$.CT_MSG.area_id')
        WHEN JSON_EXISTS(message,'$.SF_MSG') THEN JSON_VALUE(message,'$.SF_MSG.area_id')
        WHEN JSON_EXISTS(message,'$.SG_MSG') THEN JSON_VALUE(message,'$.SG_MSG.area_id')
        WHEN JSON_EXISTS(message,'$.SH_MSG') THEN JSON_VALUE(message,'$.CH_MSG.area_id')
        END AS area_id,
    CASE
        WHEN JSON_EXISTS(message,'$.CA_MSG') THEN JSON_VALUE(message,'$.CA_MSG.from')
        WHEN JSON_EXISTS(message,'$.CB_MSG') THEN JSON_VALUE(message,'$.CB_MSG.from')
        END AS from_berth,
    CASE
        WHEN JSON_EXISTS(message,'$.CA_MSG') THEN JSON_VALUE(message,'$.CA_MSG.to')
        WHEN JSON_EXISTS(message,'$.CC_MSG') THEN JSON_VALUE(message,'$.CC_MSG.to')
        END AS to_berth,
    CASE
        WHEN JSON_EXISTS(message,'$.CA_MSG') THEN JSON_VALUE(message,'$.CA_MSG.descr')
        WHEN JSON_EXISTS(message,'$.CB_MSG') THEN JSON_VALUE(message,'$.CB_MSG.descr')
        WHEN JSON_EXISTS(message,'$.CC_MSG') THEN JSON_VALUE(message,'$.CC_MSG.descr')
        END AS description,
    CASE
        WHEN JSON_EXISTS(message,'$.CT_MSG') THEN JSON_VALUE(message,'$.CA_MSG.report_time')
        END AS report_time,
    CASE
        WHEN JSON_EXISTS(message,'$.SF_MSG') THEN JSON_VALUE(message,'$.SF_MSG.address')
        WHEN JSON_EXISTS(message,'$.SG_MSG') THEN JSON_VALUE(message,'$.SG_MSG.address')
        WHEN JSON_EXISTS(message,'$.SH_MSG') THEN JSON_VALUE(message,'$.SH_MSG.address')
        END AS address,
    CASE
        WHEN JSON_EXISTS(message,'$.SF_MSG') THEN JSON_VALUE(message,'$.SF_MSG.data')
        WHEN JSON_EXISTS(message,'$.SG_MSG') THEN JSON_VALUE(message,'$.SG_MSG.data')
        WHEN JSON_EXISTS(message,'$.SH_MSG') THEN JSON_VALUE(message,'$.SH_MSG.data')
        END AS data
FROM TD CROSS JOIN UNNEST(`TEXT`) AS message;
