{{ config(
    materialized='incremental',
    unique_key='station_id',
    incremental_strategy='merge'
) 
}}

WITH unionedStations AS (
    SELECT
        start_station_id,
        NULLIF(TRIM(start_station_name), '') AS start_station_name
    FROM
        {{ ref('int_bikeshare_trips') }}
    WHERE start_station_id IS NOT NULL
    UNION ALL
    SELECT
        end_station_id,
        NULLIF(TRIM(end_station_name), '') AS end_station_name
    FROM
        {{ ref('int_bikeshare_trips') }}
    WHERE end_station_id IS NOT NULL
), 
stations AS (
    SELECT DISTINCT 
        start_station_id AS station_id,
        start_station_name AS station_name
    FROM unionedStations
    QUALIFY ROW_NUMBER() OVER (PARTITION BY start_station_id ORDER BY start_station_name) = 1
)

SELECT * FROM stationss