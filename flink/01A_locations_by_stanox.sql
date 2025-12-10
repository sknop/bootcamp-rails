-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE LOCATIONS_BY_STANOX (
                                          stanox STRING,
                                          description STRING,
                                          lat_lon row(lat double, lon double),
                                          PRIMARY KEY (stanox) NOT ENFORCED
)
    DISTRIBUTED INTO 1 BUCKETS
    WITH (
        'changelog.mode' = 'upsert',
        'kafka.cleanup-policy' = 'compact',
        'kafka.retention.time' = '0'
        )
AS
    WITH LS AS
        (   SELECT
                COALESCE(stanox,'00000') stanox,
                array_agg(cast(row(description, lat_lon) as row(description string, lat_lon row(lat double, lon double)))) details
            FROM `LOCATIONS`
            WHERE stanox <> '00000'
            GROUP BY stanox
        )
SELECT stanox, details[1].description description, details[1].lat_lon lat_lon FROM LS;