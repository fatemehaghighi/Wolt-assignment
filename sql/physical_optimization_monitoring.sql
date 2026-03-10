-- Physical optimization monitoring (BigQuery)
-- Update project_id / dataset names before use.

-- 1) Storage footprint per table (logical vs physical bytes)
select
  table_name,
  total_rows,
  total_logical_bytes,
  total_physical_bytes
from `wolt-assignment-489610.analytics_dev.INFORMATION_SCHEMA.TABLE_STORAGE`
order by total_physical_bytes desc;

-- 2) Recent query cost by destination table (last 14 days)
-- Helps check if clustered/partitioned tables are actually reducing scanned bytes.
select
  date(creation_time) as run_date,
  destination_table.dataset_id as dataset_id,
  destination_table.table_id as table_id,
  count(*) as query_count,
  round(sum(total_bytes_processed) / 1e9, 2) as processed_gb,
  round(avg(total_bytes_processed) / 1e6, 2) as avg_processed_mb
from `wolt-assignment-489610`.`region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where creation_time >= timestamp_sub(current_timestamp(), interval 14 day)
  and state = 'DONE'
  and job_type = 'QUERY'
  and destination_table is not null
  and destination_table.dataset_id = 'analytics_dev'
group by 1, 2, 3
order by processed_gb desc, query_count desc;

-- 3) Top expensive SELECT statements (last 14 days)
-- Use this to verify whether partition filter / cluster keys are being used by real workloads.
select
  creation_time,
  user_email,
  round(total_bytes_processed / 1e9, 2) as processed_gb,
  round(total_slot_ms / 1000, 2) as slot_seconds,
  query
from `wolt-assignment-489610`.`region-eu`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where creation_time >= timestamp_sub(current_timestamp(), interval 14 day)
  and state = 'DONE'
  and job_type = 'QUERY'
  and statement_type = 'SELECT'
order by total_bytes_processed desc
limit 100;

