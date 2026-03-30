{{ config(materialized='table') }}

with coordinates AS (
    SELECT
        start_station_latitude AS latitude,
        start_station_longitude AS longitude
    FROM {{ ref('int_bikeshare_trips') }}
    WHERE start_station_latitude IS NOT NULL AND start_station_longitude IS NOT NULL
    UNION ALL
    SELECT
        end_station_latitude AS latitude,
        end_station_longitude AS longitude
    FROM {{ ref('int_bikeshare_trips') }}
    WHERE end_station_latitude IS NOT NULL AND end_station_longitude IS NOT NULL
),
deduplicated_coordinates AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['latitude', 'longitude']) }} AS location_id,
        latitude,
        longitude
    FROM coordinates
)

SELECT * FROM deduplicated_coordinates