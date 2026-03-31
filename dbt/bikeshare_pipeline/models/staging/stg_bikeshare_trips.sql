SELECT

    -- Trip and bike details
    CAST(ride_id AS STRING) AS ride_id,
    CAST(rideable_type AS STRING) AS rideable_type,

    -- Timestamps
    TIMESTAMP_MICROS(CAST(started_at / 1000 AS INT64)) AS started_at,
    TIMESTAMP_MICROS(CAST(ended_at / 1000 AS INT64)) AS ended_at,

    -- Station and location data
    NULLIF(TRIM(CAST(start_station_id AS STRING)), '') AS start_station_id,
    NULLIF(TRIM(CAST(start_station_name AS STRING)), '') AS start_station_name,
    SAFE_CAST(start_lat AS FLOAT64) AS start_station_latitude,
    SAFE_CAST(start_lng AS FLOAT64) AS start_station_longitude,
    NULLIF(TRIM(CAST(end_station_id AS STRING)), '') AS end_station_id,
    NULLIF(TRIM(CAST(end_station_name AS STRING)), '') AS end_station_name,
    SAFE_CAST(end_lat AS FLOAT64) AS end_station_latitude,
    SAFE_CAST(end_lng AS FLOAT64) AS end_station_longitude,

    -- User type
    CAST(member_casual AS STRING) AS member_casual
FROM
    {{ source('raw_data', 'trips') }}

WHERE ride_id IS NOT NULL 
    AND started_at IS NOT NULL
    AND ended_at IS NOT NULL
    AND ended_at > started_at
QUALIFY ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY started_at) = 1