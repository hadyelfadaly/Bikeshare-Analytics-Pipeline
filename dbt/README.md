# Divvy Bikeshare Data Diectionary:

### 1. Columns

- `ride_id`: Unique identifier for each ride
- `rideable_type`: Type of bike used for the ride (e.g., classic bike or electric bike or ...)
- `started_at`: Timestamp when the ride started
- `ended_at`: Timestamp when the ride ended
- `start_station_name`: Name of the station where the ride started
- `start_station_id`: Unique identifier for the station where the ride started
- `end_station_name`: Name of the station where the ride ended
- `end_station_id`: Unique identifier for the station where the ride ended
- `start_lat`: Latitude of the starting location
- `start_lng`: Longitude of the starting location
- `end_lat`: Latitude of the ending location
- `end_lng`: Longitude of the ending location
- `member_casual`: Type of user (e.g., member or casual)

### 2. Data Quality Issues & Business Logic

After doing some exploration on a portion of the data (Jan 2026, Feb 2026), I found that there are many missing values in the `start_station_name`, `start_station_id`, `end_station_name`, and `end_station_id` columns. These missing values are likely due to rides that were started or ended outside of a station. Weirdly enough, most of the missing values in the `start_station_id` and `end_station_id` columns are for the 'electric_bike' `rideable_type`, and also 39% of the missing values in the `end_station_id` column has a missing `start_station_id` value and all of this 39% is for the 'electric_bike' `rideable_type`.

It doesn't stop here, after more explorations I found out that 80% and 77% of the missing values in the `start_station_id` and `end_station_id` columns respectively are actually from members other than casuals, and again they are all from electric bikes.

Doing some brainstorming on how to handle these missing values, I initially thought that since there are many `end_lat`, `end_lng`, `start_lat`, and `start_lng` available for the respective missing `start_station_id` and `end_station_id` values, I can use these latitudes and longitudes to find the nearest station for each missing value and fill it in the respective column. This way I thought I could preserve the integrity of the data and also make it more complete for analysis.

However, after investigating Divvy's actual business model, I found out this is a feature of the system, not a data pipeline error. Classic bikes must be returned to a physical dock (always recording a station ID), but electric bikes have built-in cable locks. This allows users to lock them to public city bike racks anywhere in the city (a "dockless" drop-off). Also, members frequently receive waivers for the dockless parking fee, which perfectly explains why the missing data is heavily skewed toward 'member' users.

If I were to use the coordinates to impute the nearest station ID, I would artificially inflate the inventory counts for physical docks, which would destroy the integrity of the data instead of preserving it. 

To handle this properly and preserve the raw truth of the GPS coordinates, I will handle it in the dbt pipeline:

- Standardize Nulls: Cast all empty strings `""` to true `NULL` values in the BigQuery staging layer (`stg_trips`).
- Feature Engineering: Create a new analytical column in the transformation layer to explicitly flag this behavior for downstream analysts (e.g., creating a `dropoff_type` column that flags 'Dockless Dropoff' when `end_station_id` is null for an 'electric_bike', and 'Station Dropoff' otherwise).

### 3. Investigation SQL

```SQL
SELECT COUNT(end_station_id) FROM `raw_bikeshare_data.trips` WHERE end_station_id = "";
SELECT COUNT(start_station_id) FROM `raw_bikeshare_data.trips` WHERE start_station_id = "";

SELECT rideable_type, COUNT(rideable_type) FROM `raw_bikeshare_data.trips` WHERE start_station_id = "" GROUP BY rideable_type;
SELECT rideable_type, COUNT(rideable_type) FROM `raw_bikeshare_data.trips` WHERE end_station_id = "" GROUP BY rideable_type;

SELECT start_station_id, COUNT(start_station_id)AS count FROM `raw_bikeshare_data.trips` WHERE end_station_id = "" GROUP BY start_station_id ORDER BY count DESC;
SELECT start_station_id, rideable_type, COUNT(start_station_id)AS count FROM `raw_bikeshare_data.trips` WHERE end_station_id = "" GROUP BY start_station_id, rideable_type ORDER BY count DESC;

SELECT member_casual, COUNT(member_casual) AS count FROM `raw_bikeshare_data.trips` WHERE start_station_id = "" GROUP BY member_casual;
SELECT member_casual, COUNT(member_casual) AS count FROM `raw_bikeshare_data.trips` WHERE end_station_id = "" GROUP BY member_casual;

SELECT member_casual, rideable_type, COUNT(member_casual) AS count FROM `raw_bikeshare_data.trips` WHERE start_station_id = "" GROUP BY member_casual, rideable_type;
SELECT member_casual, rideable_type, COUNT(member_casual) AS count FROM `raw_bikeshare_data.trips` WHERE end_station_id = "" GROUP BY member_casual, rideable_type;

SELECT end_lat, end_lng, COUNT(*) AS count FROM `raw_bikeshare_data.trips` WHERE end_station_id = "" GROUP BY end_lat, end_lng ORDER BY count DESC;
SELECT start_lat, start_lng, COUNT(*) AS count FROM `raw_bikeshare_data.trips` WHERE start_station_id = "" GROUP BY start_lat, start_lng ORDER BY count DESC;

SELECT * FROM `raw_bikeshare_data.trips`;
```

This is the queries I used to investigate the data quality issues in dataset.

### 4. Data Types

Using the following SQL queries, I found out that the `ride_id` column should be strictly 16 characters long. The `start_station_id` and `end_station_id` columns should be strictly 8 characters long, and they can have missing values (empty strings) which indicate that the ride started or ended outside of a station. The other columns have appropriate data types as per the data dictionary provided by Divvy.

```SQL
SELECT COUNT(LENGTH(ride_id)) FROM `raw_bikeshare_data.trips`;
SELECT LENGTH(ride_id) FROM `raw_bikeshare_data.trips`;
SELECT LENGTH(start_station_id) FROM `raw_bikeshare_data.trips` WHERE start_station_id != "";
SELECT COUNT(LENGTH(start_station_id)) FROM `raw_bikeshare_data.trips` WHERE start_station_id != "";
SELECT LENGTH(end_station_id) FROM `raw_bikeshare_data.trips` WHERE end_station_id != "";
SELECT COUNT(LENGTH(end_station_id)) FROM `raw_bikeshare_data.trips` WHERE end_station_id != "";
```

- `ride_id`: STRING(16)
- `rideable_type`: STRING
- `started_at`: DATETIME
- `ended_at`: DATETIME
- `start_station_name`: STRING
- `start_station_id`: STRING(8)
- `end_station_name`: STRING
- `end_station_id`: STRING(8)
- `start_lat`: FLOAT
- `start_lng`: FLOAT
- `end_lat`: FLOAT
- `end_lng`: FLOAT
- `member_casual`: STRING

### 5. Practical Steps

- Create the `stg_bikeshare_trips` model in the staging layer to cast correct data types and convert empty strings to NULL.
- Create `pickup_type` and `dropoff_type` columns in the intermediate transformation layer to explicitly flag station vs dockless behavior.
- Keep deduplication in the intermediate model (`int_bikeshare_trips`) and keep staging focused on cleaning and typing only.
- Create `dim_stations` and `dim_location` dimensions in the marts layer for station-level and coordinate-level analysis.
- Create the `fct_trips` model in marts to join dimensions and calculate trip duration and geodesic trip distance in meters.
- Add `schema.yml` tests (unique, not_null, accepted_values, relationships, and non-negative metric checks) for intermediate and marts models.

### 6. Latest Pipeline Updates

- In `int_bikeshare_trips`, the model now generates `trip_id`, keeps cleaned station/location fields, and adds `pickup_type` + `dropoff_type` using business-aware macros.
- In `fct_trips`, joins are done with `LEFT JOIN` to preserve dockless/unknown station behavior instead of dropping those records.
- `trip_duration_minutes` is calculated using BigQuery `TIMESTAMP_DIFF`.
- `trip_distance_meters` is calculated using `ST_DISTANCE(ST_GEOGPOINT(...), ST_GEOGPOINT(...))` through a reusable macro.
- A very small portion of classic bike rows can still have null station IDs, and these are kept as-is as potential operational exceptions (not force-imputed).
- Data quality tests were added for categorical columns like `rideable_type`, `member_casual`, `pickup_type`, and `dropoff_type`.