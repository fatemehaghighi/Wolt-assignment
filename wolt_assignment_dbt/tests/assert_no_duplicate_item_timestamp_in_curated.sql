-- Curated stream must contain at most one row per item_key + event timestamp.

select
    item_key,
    time_log_created_utc,
    count(*) as row_cnt
from {{ ref('int_wolt_item_logs_curated') }}
group by 1, 2
having count(*) > 1

