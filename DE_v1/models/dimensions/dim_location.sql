-- models/dimensions/dim_location.sql

  with distinct_locations as (
      select distinct
          trim(location) as location_name
      from {{ ref('silver_curated') }}
      where location is not null
        and trim(location) <> ''
  ),

  numbered_locations as (
      select
          row_number() over (order by location_name) as location_key,
          location_name,
          'No'::text as is_unknown
      from distinct_locations
  )

  select
      -1 as location_key,
      'Unknown Location'::text as location_name,
      'Yes'::text as is_unknown

  union all

  select
      location_key,
      location_name,
      is_unknown
  from numbered_locations
