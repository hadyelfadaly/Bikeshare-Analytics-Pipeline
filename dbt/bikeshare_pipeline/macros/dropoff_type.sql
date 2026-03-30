{% macro dropoff_type(rideable_type, end_station_id) %}
    CASE
        WHEN {{ rideable_type }} = 'electric_bike' AND {{ end_station_id }} IS NULL THEN 'dockless_dropoff'
        WHEN {{ end_station_id }} IS NULL THEN 'missing_station_unknown'
        ELSE 'station_dropoff'
    END AS dropoff_type
{% endmacro %}
