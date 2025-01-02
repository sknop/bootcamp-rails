-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE FLINK_LOCATIONS_BY_STANOX (
                                          stanox STRING,
                                          description STRING,
                                          lat_lon row(lat double, lon double),
                                          PRIMARY KEY (stanox) NOT ENFORCED
)
    WITH (
        'changelog.mode' = 'upsert',
        'kafka.cleanup-policy' = 'compact',
        'kafka.retention.time' = '0'
        )
AS
    WITH LOCATION_BY_STANOX AS
        ( SELECT
        COALESCE(stanox,'00000') stanox,
    array_agg(cast(row(description, lat_lon) as row(description string, lat_lon row(lat double, lon double)))) details
    FROM `FLINK_LOCATIONS`
    WHERE stanox <> '00000'
    GROUP BY stanox
    )
SELECT stanox, details[1].description description, details[1].lat_lon lat_lon FROM LOCATION_BY_STANOX;