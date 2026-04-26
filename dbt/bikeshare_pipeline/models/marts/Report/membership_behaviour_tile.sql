SELECT t.member_casual, AVG(t.trip_duration_minutes) AS avg_trip_duration_minutes, AVG(t.trip_distance_meters) AS avg_trip_distance_meters
FROM {{ ref('fct_trips') }} AS t
GROUP BY t.member_casual