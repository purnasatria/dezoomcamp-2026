# Data Warehouse

## Material

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/03-data-warehouse

## Homework

https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/cohorts/2026/03-data-warehouse

### Prerequisite

- [x] Create `requirements.txt` to handle all library installation
- [x] Prepare key to access GCS
- [x] setup env and install requirements
- [x] run script `./load_yellow_taxi_data.py`

### Answer

```sql
-- # After load data to GCS,
-- # 1. Create external table
-- # 2. Create native table
-- # Uncomment the each query to answer the question

-- =====================================================================================================

-- Question 1
-- SELECT count(1) FROM `personal-431119.ny_taxi.yellow_trip_data`;

-- =====================================================================================================

-- Question 2
-- External Table
-- SELECT COUNT(DISTINCT(PULocationID)) FROM `personal-431119.ny_taxi.yellow_trip_data`;

-- Materialized Table
-- SELECT COUNT(DISTINCT(PULocationID)) FROM `personal-431119.ny_taxi.yellow_trip_data_native`;

-- =====================================================================================================

-- Question 3
-- Estimated 155.12 MB
-- SELECT PULocationID FROM `personal-431119.ny_taxi.yellow_trip_data_native`;

-- Estimated 310.24 MB
-- SELECT PULocationID, DOLocationID FROM `personal-431119.ny_taxi.yellow_trip_data_native`;

-- =====================================================================================================

-- Question 4
-- SELECT count(1) FROM `personal-431119.ny_taxi.yellow_trip_data_native`
-- WHERE fare_amount = 0;

-- =====================================================================================================

-- Question 5
-- CREATE OR REPLACE TABLE `personal-431119.ny_taxi.yellow_trip_data_partitioned_clustered`
-- PARTITION BY DATE(tpep_dropoff_datetime)
-- CLUSTER BY VendorID
-- AS
-- SELECT *
-- FROM `personal-431119.ny_taxi.yellow_trip_data_native`;

-- =====================================================================================================

-- Question 6
-- Query on non-partitioned table
--  Estimated 310 MB
-- SELECT DISTINCT VendorID
-- FROM `personal-431119.ny_taxi.yellow_trip_data_native`
-- WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- Query on partitioned and clustered table
-- Estimated 26 MB
-- SELECT DISTINCT VendorID
-- FROM `personal-431119.ny_taxi.yellow_trip_data_partitioned_clustered`
-- WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

-- =====================================================================================================

-- Question 7
-- Our external table data still stored in GCS
-- BQ only store the metadata
-- Reference: https://docs.cloud.google.com/bigquery/docs/external-data-sources#external_data_source_feature_comparison

-- =====================================================================================================

-- Question 8
-- Answer: False

-- Because:
-- Clustering adds overhead to data ingestion and table maintenance
-- Only beneficial for large tables (typically > 1GB)
-- Only helps if you frequently filter/sort on the clustered columns
-- Small tables or tables without common query patterns don't benefit
-- Can actually slow down queries on small tables due to metadata overhead

-- =====================================================================================================

-- Question 9
-- Estimated 0 Byte
-- SELECT COUNT(*) FROM `personal-431119.ny_taxi.yellow_trip_data_native`;
-- Estimated 0 Byte
-- SELECT COUNT(*) FROM `personal-431119.ny_taxi.yellow_trip_data_partitioned_clustered`;

-- 0 MB
-- Because materialized views pre-compute and cache results.
-- COUNT(*) query is already aggregated in the materialized view's metadata,
-- so it requires minimal to no scanning of the actual data.

```
