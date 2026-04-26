{% macro dropoffpickup_type(rideable_type, station_id, type) %}
    CASE
        WHEN {{ rideable_type }} = 'electric_bike' AND {{ station_id }} IS NULL THEN 'dockless_{{ type }}'
        WHEN {{ station_id }} IS NULL THEN 'missing_station_unknown'
        ELSE 'station_{{ type }}'
    END AS {{ type }}_type
{% endmacro %}