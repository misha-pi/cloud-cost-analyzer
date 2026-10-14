-- GCP billing rows from the local parquet files (DuckDB mode only).
-- Works with the standard or the resource-level export: the standard export has no
-- resource columns, so they are added as NULL (the metrics view uses them as dimensions).
SELECT * FROM read_parquet('data/gcp_costs/bigquery_billing_table/*.parquet')
UNION ALL BY NAME
SELECT NULL::VARCHAR AS resource__name, NULL::VARCHAR AS resource__global_name WHERE false
