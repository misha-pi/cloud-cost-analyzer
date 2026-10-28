-- @materialize: true
-- Materialized: stored once as a DuckDB table (rebuilt when the data is refreshed, see `make rill-refresh`)
-- instead of re-reading the GCP parquet on every dashboard query.
-- Resource labels used to assign GCP billing rows to apps, one row per billing row (DuckDB mode only).
-- k8s-namespace and k8s-workload-name (the Deployment, StatefulSet, ... that used the cost) come from GKE
-- cost allocation (which also labels the GKE node VM costs); app is our own resource label. Kept as its own single-source model so Rill resolves the data/ path.
-- Only billing rows with one of these labels get a row: the other label keys (most of the rows) are
-- filtered out before grouping, which keeps the build small. gcp_app_costs LEFT JOINs this model, so a
-- missing row means the same as NULL labels.
SELECT
  _dlt_parent_id,
  MAX(CASE WHEN key = 'k8s-namespace' THEN value END) AS k8s_namespace,
  MAX(CASE WHEN key = 'k8s-workload-name' THEN value END) AS k8s_workload,
  MAX(CASE WHEN key = 'app' THEN value END) AS app_label
FROM read_parquet('data/gcp_costs/bigquery_billing_table__labels/*.parquet', union_by_name = true)
WHERE key IN ('k8s-namespace', 'k8s-workload-name', 'app')
GROUP BY _dlt_parent_id
