{%- macro trip_distance(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude) -%}
    CASE
        WHEN {{ start_station_latitude }} IS NULL OR {{ start_station_longitude }} IS NULL
            OR {{ end_station_latitude }} IS NULL OR {{ end_station_longitude }} IS NULL
        THEN NULL
        ELSE
            st_distance(
                st_geogpoint({{ start_station_longitude }}, {{ start_station_latitude }}),
                st_geogpoint({{ end_station_longitude }}, {{ end_station_latitude }})
            )
    END AS trip_distance_meters
{%- endmacro -%}