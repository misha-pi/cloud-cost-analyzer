-- @materialize: true
-- Materialized: stored once as a DuckDB table (rebuilt when the data is refreshed, see `make rill-refresh`).
-- Cost per day, app and service for the "Cost Summary" page (DuckDB mode only): the GCP rows of
-- gcp_app_costs plus MongoDB Atlas, like the "Atlas" and "Atlas Pro" rows of billing-helper's monthly sheet.
--
-- MongoDB: each Atlas project goes to the app whose name is in the project name, spaces removed
-- ("Map of Pi - Production" -> mapofpi), with the same app list and order as the GCP name rules, then
-- "main app"; projects matching no app go to "other".
-- MongoDB Pro: org-level Atlas lines (no project; "All Projects" in billing-helper) are spread over the apps
-- by their share of that month's MongoDB cost, as rows dated on the 1st. There are none in the data so far.
WITH atlas_rules AS (
  SELECT priority, pattern, app FROM {{ ref "gcp_app_rules" }} WHERE match_on = 'name'
  UNION ALL
  SELECT 900, '%mainapp%', 'main app'
),

atlas_project_app AS (
  SELECT p.project_name, COALESCE(ARG_MIN(r.app, r.priority), 'other') AS app
  FROM (SELECT DISTINCT project_name FROM {{ ref "atlas_costs" }} WHERE project_name <> '(no project)') p
  LEFT JOIN atlas_rules r ON REPLACE(LOWER(p.project_name), ' ', '') LIKE r.pattern
  GROUP BY p.project_name
),

mongodb AS (
  SELECT c.date, CAST(DATE_TRUNC('month', c.date) AS DATE) AS month_start, a.app, SUM(c.cost) AS net_cost
  FROM {{ ref "atlas_costs" }} c
  JOIN atlas_project_app a USING (project_name)
  GROUP BY ALL
),

mongodb_pro AS (
  SELECT CAST(DATE_TRUNC('month', date) AS DATE) AS month_start, SUM(cost) AS net_cost
  FROM {{ ref "atlas_costs" }}
  WHERE project_name = '(no project)'
  GROUP BY month_start
),

mongodb_share AS (
  SELECT month_start, app, SUM(net_cost) / NULLIF(SUM(SUM(net_cost)) OVER (PARTITION BY month_start), 0) AS share
  FROM mongodb
  GROUP BY month_start, app
)

SELECT date, app, service_name, 'GCP' AS provider, SUM(net_cost) AS net_cost
FROM {{ ref "gcp_app_costs" }}
GROUP BY ALL

UNION ALL

SELECT date, app, 'MongoDB' AS service_name, 'MongoDB Atlas' AS provider, net_cost
FROM mongodb

UNION ALL

SELECT p.month_start AS date, s.app, 'MongoDB Pro' AS service_name, 'MongoDB Atlas' AS provider, p.net_cost * s.share AS net_cost
FROM mongodb_pro p
JOIN mongodb_share s USING (month_start)
