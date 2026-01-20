{{ config(materialized="table") }}

select
  species,
  coalesce(sex, 'unknown') as sex,
  count(*) as penguin_count,
  round(avg(body_mass_g)::numeric, 2) as avg_body_mass_g,
  round(avg(flipper_length_mm)::numeric, 2) as avg_flipper_length_mm
from {{ ref("stg_penguins") }}
group by 1,2
order by 1,2
