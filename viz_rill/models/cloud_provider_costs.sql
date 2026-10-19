-- GCP and MongoDB Atlas cost per day side by side (DuckDB mode only).
-- GCP is net cost after credits (as on the GCP pages); Atlas is the billed line item cost.
-- service: GCP service / Atlas category. component: GCP project / Atlas project and cluster.
SELECT
  date,
  STRFTIME(date, '%Y-%m') AS month,
  CAST(YEAR(date) AS VARCHAR) AS year,
  'GCP' AS provider,
  COALESCE(service_name, 'Unknown') AS service,
  COALESCE(project_id, 'Unknown') AS component,
  net_cost AS cost
FROM {{ ref "gcp_costs" }}
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
