-- MongoDB Atlas cost per day, project, cluster and SKU (DuckDB mode only).
-- `cost` is what Atlas bills for the line (there are no negative credit lines); for a closed month
-- the costs add up to the invoice subtotal (see atlas_invoice_totals).
--
-- Cluster names repeat across projects (the same name in Production and Staging), so
-- project_cluster is the unique cluster. Org-level lines (Support, Charts, Data Lake, ...) have
-- cluster '--n/a--' and show as "(no cluster)"; Support lines have no project and show as "(no project)".
-- month and year are text columns for the monthly and yearly tables; the charts use `date` with the
-- dashboard's time grain (day, week, month, year).
SELECT
  date,
  STRFTIME(date, '%Y-%m') AS month,
  CAST(YEAR(date) AS VARCHAR) AS year,
  invoice_id,
  project_id,
  COALESCE(project_name, '(no project)') AS project_name,
  -- Environment from the last " - " part of the project name, matched as a whole word (so "Devices"
  -- is not "dev"): "Main App - MS - Production" -> Production, app "Main App - MS". The same rule is
  -- used for GCP project IDs (metrics/gcp_cost_metrics.yaml).
  CASE WHEN project_name IS NULL THEN '(org level)'
       WHEN REGEXP_MATCHES(LOWER(env_part), '(^|[^a-z0-9])(prod|production|prd)([^a-z0-9]|$)') THEN 'Production'
       WHEN REGEXP_MATCHES(LOWER(env_part), '(^|[^a-z0-9])(staging|stage|stg)([^a-z0-9]|$)') THEN 'Staging'
       WHEN REGEXP_MATCHES(LOWER(env_part), '(^|[^a-z0-9])(dev|development)([^a-z0-9]|$)') THEN 'Development'
       WHEN REGEXP_MATCHES(LOWER(env_part), '(^|[^a-z0-9])(test|testing|qa|e2e)([^a-z0-9]|$)') THEN 'Test'
       ELSE 'Other' END AS environment,
  CASE WHEN project_name IS NULL THEN '(no project)'
       WHEN project_name LIKE '% - %' THEN REGEXP_REPLACE(project_name, '\s+-\s+[^-]*$', '')
       ELSE project_name END AS app,
  cluster,
  is_cluster_cost,
  CASE WHEN is_cluster_cost THEN cluster ELSE '(no cluster)' END AS cluster_label,
  -- The same cluster name exists in several projects (Production, Staging, ...): label it with its
  -- environment, e.g. "kyc (Production)".
  CASE WHEN is_cluster_cost THEN cluster || ' (' || environment || ')' ELSE '(no cluster)' END AS cluster_env,
  COALESCE(project_name, '(no project)') || ' / '
    || CASE WHEN is_cluster_cost THEN cluster ELSE '(no cluster)' END AS project_cluster,
  category,
  sku,
  instance,
  NULLIF(instance, 'non-instance') AS instance_size,  -- NULL for lines that are not an instance
  provider,
  unit,
  quantity,
  CAST(unit_price_dollars AS DOUBLE) AS unit_price_dollars,
  CAST(cost AS DOUBLE) AS cost
FROM (
  SELECT *, TRIM(REGEXP_EXTRACT(project_name, '([^-]*)$', 1)) AS env_part
  FROM {{ ref "atlas_line_items_raw" }}
)
WHERE cost IS NOT NULL
