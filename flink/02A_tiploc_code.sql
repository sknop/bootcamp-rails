-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE TIPLOC (
                        tiploc_code STRING,
                        nalco STRING,
                        stanox STRING,
                        crs_code STRING,
                        `description` STRING,
                        tps_description STRING,
                        PRIMARY KEY (tiploc_code) NOT ENFORCED
)
    WITH (
        'changelog.mode' = 'upsert',
        'kafka.cleanup-policy' = 'compact',
        'kafka.retention.time' = '0'
        )
AS
    WITH CIF_FULL_DAILY_TIPLOC_JSON AS (
        SELECT JSON_QUERY(CAST(val as STRING),'$.TiplocV1') as tiploc FROM `CIF_FULL_DAILY`
    WHERE
    JSON_QUERY(CAST(val as STRING),'$.TiplocV1') IS NOT NULL
    )
SELECT
    COALESCE(JSON_VALUE(tiploc,'$.tiploc_code'),'No TIPLOC Code') AS tiploc_code,
    JSON_VALUE(tiploc,'$.naloc') AS nalco,
    JSON_VALUE(tiploc,'$.stanox') AS stanox,
    JSON_VALUE(tiploc,'$.crs_code') AS crs_code,
    JSON_VALUE(tiploc,'$.description') AS `description`,
    JSON_VALUE(tiploc,'$.tps_description') AS tps_description
FROM CIF_FULL_DAILY_TIPLOC_JSON;
