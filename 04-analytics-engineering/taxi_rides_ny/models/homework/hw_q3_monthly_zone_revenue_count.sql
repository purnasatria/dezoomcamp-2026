select count(*) as record_count
from {{ ref('fct_monthly_zone_revenue') }}
