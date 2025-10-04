{{ config(materialized='table') }}
select
  *
from {{ source('nyc_taxi', 'tlc_green_trips_2014') }}
TABLESAMPLE SYSTEM (0.1 PERCENT);