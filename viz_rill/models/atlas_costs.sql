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
  -- "Orders - Production" -> app "Orders", environment "Production".
  COALESCE(NULLIF(TRIM(SPLIT_PART(project_name, ' - ', 1)), ''), '(no project)') AS app,
  CASE WHEN project_name IS NULL THEN '(org level)'
       ELSE COALESCE(NULLIF(TRIM(SPLIT_PART(project_name, ' - ', 2)), ''), '(none)') END AS environment,
  cluster,
  is_cluster_cost,
  CASE WHEN is_cluster_cost THEN cluster ELSE '(no cluster)' END AS cluster_label,
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
FROM {{ ref "atlas_line_items_raw" }}
WHERE cost IS NOT NULL
