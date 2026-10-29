-- GCP cost rows from the local parquet files (DuckDB mode only), written by pipelines/gcp_storage.py:
-- one row per usage day and dimensions for recent months (grain 'day'), per usage month for older months
-- (grain 'month', date = the 1st). The hourly file (no grain) holds single billing rows and is left out.
SELECT * FROM read_parquet('data/gcp_costs/*/**/*.parquet', union_by_name = true)
WHERE grain IN ('day', 'month')
