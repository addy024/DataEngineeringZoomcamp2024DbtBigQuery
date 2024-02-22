{{ config(materialized="table") }}
--
with
    fhv_trips as (select * from {{ ref("stg_staging__fhv") }}),
    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')

select
    fhv_trips.dispatching_base_num,
    fhv_trips.pickup_datetime,
    fhv_trips.dropoff_datetime,
    fhv_trips.pulocationid,
    fhv_trips.dolocationid,
    fhv_trips.sr_flag,
    fhv_trips.affiliated_base_number,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
from fhv_trips
inner join dim_zones as pickup_zone on fhv_trips.pulocationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone on fhv_trips.dolocationid = dropoff_zone.locationid
where extract(year from pickup_datetime) = 2019
