SELECT l.latitude, l.longitude, t.pickup_type, t.trip_id
FROM {{ref('dim_location')}} AS l
INNER JOIN {{ref('fct_trips')}} AS t ON l.location_id = t.start_location_id