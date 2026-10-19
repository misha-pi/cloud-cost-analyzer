-- @materialize: true
-- Materialized: stored once as a DuckDB table (rebuilt when the data is refreshed, see `make rill-refresh`)
-- instead of re-reading the GCP parquet on every dashboard query.
-- Resource labels used to assign GCP billing rows to apps, one row per billing row (DuckDB mode only).
-- k8s-namespace comes from GKE cost allocation (it also labels the GKE node VM costs per namespace);
-- app is our own resource label. Kept as its own single-source model so Rill resolves the data/ path.
SELECT
  _dlt_parent_id,
  MAX(CASE WHEN key = 'k8s-namespace' THEN value END) AS k8s_namespace,
  MAX(CASE WHEN key = 'app' THEN value END) AS app_label
FROM read_parquet('data/gcp_costs/bigquery_billing_table__labels/*.parquet', union_by_name = true)
GROUP BY _dlt_parent_id
