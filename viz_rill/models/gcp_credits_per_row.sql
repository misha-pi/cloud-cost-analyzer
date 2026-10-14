-- Total credits per GCP billing row (DuckDB mode only). `cost` is before credits and a
-- row can have several credits, so they are summed per row before gcp_costs joins them;
-- joining credit rows directly would repeat `cost`.
SELECT _dlt_parent_id, SUM(amount) AS credits_total
FROM read_parquet('data/gcp_costs/bigquery_billing_table__credits/*.parquet', union_by_name = true)
GROUP BY _dlt_parent_id
