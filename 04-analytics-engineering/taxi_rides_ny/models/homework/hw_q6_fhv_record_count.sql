select count(*) as record_count
from {{ ref('stg_fhv_tripdata') }}
