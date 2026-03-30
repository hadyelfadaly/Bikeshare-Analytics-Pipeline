{% macro pickup_type(rideable_type, start_station_id) %}
    CASE
        WHEN {{ rideable_type }} = 'electric_bike' AND {{ start_station_id }} IS NULL THEN 'dockless_pickup'
        WHEN {{ start_station_id }} IS NULL THEN 'missing_station_unknown'
        ELSE 'station_pickup'
    END AS pickup_type
{% endmacro %}
