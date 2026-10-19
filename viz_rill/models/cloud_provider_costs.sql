-- @materialize: true
-- Materialized: stored once as a DuckDB table (rebuilt when the data is refreshed, see `make rill-refresh`)
-- instead of re-reading the GCP parquet on every dashboard query.
-- GCP and MongoDB Atlas cost per day side by side (DuckDB mode only).
-- GCP is net cost after credits (as on the GCP pages); Atlas is the billed line item cost.
-- service: GCP service / Atlas category. component: GCP project / Atlas project and cluster.
-- Stored as daily totals per provider, service and component: same totals, far fewer rows than the hourly
-- GCP billing rows.
SELECT
  date,
  STRFTIME(date, '%Y-%m') AS month,
  CAST(YEAR(date) AS VARCHAR) AS year,
  'GCP' AS provider,
  COALESCE(service_name, 'Unknown') AS service,
  COALESCE(project_id, 'Unknown') AS component,
  SUM(net_cost) AS cost
FROM {{ ref "gcp_costs" }}
GROUP BY ALL
UNION ALL
SELECT
  date,
  month,
  year,
  'MongoDB Atlas' AS provider,
  COALESCE(category, 'Unknown') AS service,
  project_cluster AS component,
  cost
FROM {{ ref "atlas_costs" }}
