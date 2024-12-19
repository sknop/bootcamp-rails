-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE FLINK_LOCATIONS (
                                 `tiploc` STRING,
                                 `name` STRING,
                                 `description` STRING,
                                 `location_id` STRING,
                                 `crs` STRING,
                                 `nlc` STRING ,
                                 `stanox` STRING,
                                 `lat_lon` ROW(lat double , lon double),
                                 `notes` STRING,
                                 `is_off_network` STRING,
                                 `timing_point_type` STRING,
                                 PRIMARY KEY (`tiploc`) NOT ENFORCED
                             )
    WITH (
        'changelog.mode' = 'upsert',
        'kafka.cleanup-policy' = 'compact',
        'kafka.retention.time' = '0'
    )
AS
SELECT
    `tiploc`,
    `name`,
    `description`,
    `location_id`,
    crs,
    nlc,
    LPAD(stanox,5,'00000') as stanox,
    ROW(TRY_CAST(latitude AS DOUBLE), TRY_CAST(longitude AS DOUBLE)) AS lat_lon,
    notes,
    isOffNetwork AS is_off_network,
    timingPointType as timing_point_type
FROM LOCATIONS_RAW
WHERE `tiploc` <> '';