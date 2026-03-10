-- SCD2 validity windows must be strictly forward-moving.

select *
from {{ ref('int_wolt_item_scd2') }}
where valid_to_utc <= valid_from_utc

