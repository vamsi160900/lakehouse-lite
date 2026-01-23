{{ config(materialized="view") }}

select
  trim(species) as species,
  trim(island) as island,

  cast(bill_length_mm as double precision) as bill_length_mm,
  cast(bill_depth_mm  as double precision) as bill_depth_mm,
  cast(flipper_length_mm as double precision) as flipper_length_mm,
  cast(body_mass_g as double precision) as body_mass_g,

  case
    when sex is null then 'unknown'
    when trim(sex) = '' then 'unknown'
    else upper(trim(sex))
  end as sex

from raw.penguins
