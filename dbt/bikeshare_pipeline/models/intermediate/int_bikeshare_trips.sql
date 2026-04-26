WITH cleaned_trips AS (
    SELECT
        
        -- Generate a unique trip ID using the trip's ride ID, start time, end time, and station IDs
        {{ dbt_utils.generate_surrogate_key(['ride_id', 'started_at', 'ended_at', 'start_station_id', 'end_station_id']) }} AS trip_id,

        ride_id,
        rideable_type,

        -- Timestamps
        started_at,
        ended_at,

        -- Station and location data
        start_station_id,
        start_station_name,
        start_station_latitude,
        start_station_longitude,
        end_station_id,
        end_station_name,
        end_station_latitude,
        end_station_longitude,
        {{ dropoffpickup_type('rideable_type', 'start_station_id', 'pickup') }},
        {{ dropoffpickup_type('rideable_type', 'end_station_id', 'dropoff') }},
        member_casual
    FROM
        {{ ref('stg_bikeshare_trips') }}
)

-- Deduplicate trips by keeping only earliest record for each trip_id, based on the ended_at timestamp
SELECT * FROM cleaned_trips
QUALIFY row_number() OVER (PARTITION BY ride_id, start_station_id, end_station_id ORDER BY ended_at) = 1