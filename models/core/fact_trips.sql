{{ config(materialized="table") }}

with
    green_tripdata as (
        select
            stg_green_tripdata.tripid,
            stg_green_tripdata.vendor_id,
            stg_green_tripdata.rate_code,
            cast(stg_green_tripdata.pickup_location_id as integer) as pickup_location_id,
            cast(stg_green_tripdata.dropoff_location_id as integer) as dropoff_location_id,
            stg_green_tripdata.pickup_datetime,
            stg_green_tripdata.dropoff_datetime,
            stg_green_tripdata.store_and_fwd_flag,
            stg_green_tripdata.passenger_count,
            stg_green_tripdata.trip_distance,
            stg_green_tripdata.fare_amount,
            stg_green_tripdata.extra,
            stg_green_tripdata.mta_tax,
            stg_green_tripdata.tip_amount,
            stg_green_tripdata.tolls_amount,
            stg_green_tripdata.total_amount,
            stg_green_tripdata.payment_type,
            stg_green_tripdata.payment_type_descripted,
            'Green' as service_type
        from {{ ref("stg_green_tripdata") }}
    ),
    yellow_tripdata as (
        select
            stg_staging__yellow_tripdata.tripid,
            stg_staging__yellow_tripdata.vendor_id,
            stg_staging__yellow_tripdata.rate_code,
            cast(stg_staging__yellow_tripdata.pickup_location_id as integer) as pickup_location_id,
            cast(stg_staging__yellow_tripdata.dropoff_location_id as integer) as dropoff_location_id,
            stg_staging__yellow_tripdata.pickup_datetime,
            stg_staging__yellow_tripdata.dropoff_datetime,
            stg_staging__yellow_tripdata.store_and_fwd_flag,
            stg_staging__yellow_tripdata.passenger_count,
            stg_staging__yellow_tripdata.trip_distance,
            stg_staging__yellow_tripdata.fare_amount,
            stg_staging__yellow_tripdata.extra,
            stg_staging__yellow_tripdata.mta_tax,
            stg_staging__yellow_tripdata.tip_amount,
            stg_staging__yellow_tripdata.tolls_amount,
            stg_staging__yellow_tripdata.total_amount,
            stg_staging__yellow_tripdata.payment_type,
            stg_staging__yellow_tripdata.payment_type_descripted,
            'Yellow' as service_type
        from {{ ref("stg_staging__yellow_tripdata") }}
    ),
    trips_unioned as (
        select *
        from green_tripdata
        union all
        select *
        from yellow_tripdata
    ),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')
select
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    trips_unioned.tripid,
    trips_unioned.vendor_id,
    trips_unioned.rate_code,
    trips_unioned.pickup_location_id,
    trips_unioned.dropoff_location_id,
    trips_unioned.pickup_datetime,
    trips_unioned.dropoff_datetime,
    trips_unioned.store_and_fwd_flag,
    trips_unioned.passenger_count,
    trips_unioned.trip_distance,
    trips_unioned.fare_amount,
    trips_unioned.extra,
    trips_unioned.mta_tax,
    trips_unioned.tip_amount,
    trips_unioned.tolls_amount,
    trips_unioned.total_amount,
    trips_unioned.payment_type,
    trips_unioned.payment_type_descripted
from trips_unioned
inner join
    dim_zones as pickup_zone
    on trips_unioned.pickup_location_id = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on trips_unioned.dropoff_location_id = dropoff_zone.locationid
