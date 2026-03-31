SELECT l.latitude, l.longitude, t.pickup_type, COUNT(*) AS trip_count
FROM {{ref('dim_location')}} AS l
INNER JOIN {{ref('fct_trips')}} AS t ON l.location_id = t.start_location_id
GROUP BY l.latitude, l.longitude, t.pickup_type