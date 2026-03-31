{{
    config(
        materialized="incremental",
        unique_key="trip_id",
        incremental_strategy="merge",
        on_schema_change="append_new_columns",
        partition_by={
            "field": "started_at",
            "data_type": "timestamp",
            "granularity": "month",
        },
        cluster_by=["member_casual", "rideable_type"],
    )
}}

with
    alltrips as (select * from {{ ref("int_bikeshare_trips") }}),
    trips as (
        select
            a.trip_id,
            a.ride_id,
            l1.location_id as start_location_id,
            l2.location_id as end_location_id,
            s1.station_id as start_station_id,
            s2.station_id as end_station_id,
            a.rideable_type,
            a.started_at,
            a.ended_at,
            timestamp_diff(a.ended_at, a.started_at, minute) as trip_duration_minutes,
            {{
                trip_distance(
                    "l1.latitude", "l1.longitude", "l2.latitude", "l2.longitude"
                )
            }},
            {{ pickup_type("a.rideable_type", "a.start_station_id") }},
            {{ dropoff_type("a.rideable_type", "a.end_station_id") }},
            a.member_casual
        from alltrips as a
        left join {{ ref("dim_stations") }} as s1 on a.start_station_id = s1.station_id
        left join {{ ref("dim_stations") }} as s2 on a.end_station_id = s2.station_id
        left join
            {{ ref("dim_location") }} as l1
            on a.start_station_latitude = l1.latitude
            and a.start_station_longitude = l1.longitude
        left join
            {{ ref("dim_location") }} as l2
            on a.end_station_latitude = l2.latitude
            and a.end_station_longitude = l2.longitude
    )

select *
from trips

{% if is_incremental() %}
    -- Only process new trips based on started_at timestamp, assuming that new trips
    -- will have a later started_at than existing ones
    where
        started_at
        >= (select coalesce(max(started_at), timestamp('1900-01-01')) from {{ this }})
{% endif %}
