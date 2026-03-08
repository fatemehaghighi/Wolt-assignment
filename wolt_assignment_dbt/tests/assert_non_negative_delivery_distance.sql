select *
from {{ ref('fct_order') }}
where delivery_distance_line_meters < 0
