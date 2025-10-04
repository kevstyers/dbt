select 
  vendor_id
  , pickup_datetime
  , dropoff_datetime
  , TIMESTAMP_DIFF(cast(dropoff_datetime as timestamp), cast(pickup_datetime as timestamp), minute) as trip_duration_minutes
  , store_and_fwd_flag
  , rate_code
  , case
      when rate_code = '1' then 'Standard Rate'
      when rate_code = '2' then 'JFK'
      when rate_code = '3' then 'Newark'
      when rate_code = '4' then 'Nassau or Winchester'
      when rate_code = '5' then 'Negotiated Rate'
      when rate_code = '6' then 'Group Ride'
    else NULL
  end as rate_code_description
  , passenger_count
  , trip_distance
  , fare_amount
  , extra
  , mta_tax
  , tip_amount
  , tolls_amount
  , ehail_fee
  , total_amount
  , payment_type
  , case 
      when payment_type = '1' then 'Credit Card'
      when payment_type = '2' then 'Cash'
      when payment_type = '3' then 'No Charge'
      when payment_type = '4' then 'Dispute'
      when payment_type = '5' then 'Unknown'
      when payment_type = '6' then 'Voided'
    else NULL
  end as payment_type_description
  , distance_between_service
  , time_between_service
  , trip_type
  , imp_surcharge
  , pickup_location_id
  , tzid_pick.zone_name as pickup_zone
  , tzid_pick.borough as pickup_borough
  , dropoff_location_id
  , tzid_drop.zone_name as dropoff_zone
  , tzid_drop.borough as dropoff_borough
  , data_file_month
FROM {{ ref('taxis_ingest') }} as sample
LEFT JOIN 
  {{ source('nyc_taxi', 'taxi_zone_geom') }} as tzid_pick
  on sample.pickup_location_id = tzid_pick.zone_id
LEFT JOIN 
  {{ source('nyc_taxi', 'taxi_zone_geom') }} as tzid_drop
  on sample.pickup_location_id = tzid_drop.zone_id
