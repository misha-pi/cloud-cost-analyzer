-- @materialize: true
-- Materialized: stored once as a DuckDB table (rebuilt when the data is refreshed, see `make rill-refresh`)
-- instead of re-reading the GCP parquet on every dashboard query.
-- GCP net cost per app (DuckDB mode only): every billing row of gcp_costs gets an app from gcp_app_rules.
-- Net cost is after credits, like billing-helper's sheet. Feeds the "GCP Cost by Service and App" page,
-- which replaces the monthly sheet built with billing-helper.
--
-- Support is billed for the whole account, so (like billing-helper) each month's support cost is
-- spread over the apps by their share of that month's net cost, as "Support" rows dated on the 1st.
-- matched_by says which rule assigned the row ("unmatched" rows go to "other"), to explain differences.
--
-- The rules only look at project, service and app_key, so they are matched once per distinct combination
-- (a few thousand) instead of once per billing row (millions), and the result is stored as daily totals.
WITH billing_rows AS (
  SELECT
    c.date,
    CAST(DATE_TRUNC('month', c.date) AS DATE) AS month,
    c.project_id,
    c.service_name,
    c.net_cost,
    LOWER(COALESCE(l.k8s_namespace, l.app_label, c.resource__name, '')) AS app_key
  FROM {{ ref "gcp_costs" }} c
  LEFT JOIN {{ ref "gcp_labels_per_row" }} l ON l._dlt_parent_id = c._dlt_id
),

match_keys AS (
  SELECT DISTINCT project_id, service_name, app_key FROM billing_rows
),

first_match AS (
  SELECT
    k.project_id,
    k.service_name,
    k.app_key,
    ARG_MIN(r.app, r.priority) AS app,
    ARG_MIN(r.match_on || ': ' || r.pattern, r.priority) AS matched_by
  FROM match_keys k
  JOIN {{ ref "gcp_app_rules" }} r
    ON (r.match_on = 'project' AND LOWER(k.project_id) LIKE r.pattern)
    OR (r.match_on = 'service' AND LOWER(k.service_name) LIKE r.pattern)
    OR (r.match_on = 'name' AND k.app_key LIKE r.pattern)
  GROUP BY k.project_id, k.service_name, k.app_key
),

assigned AS (
  SELECT
    b.date,
    b.month,
    b.service_name,
    b.app_key,
    COALESCE(m.app, 'other') AS app,
    COALESCE(m.matched_by, 'unmatched') AS matched_by,
    SUM(b.net_cost) AS net_cost
  FROM billing_rows b
  LEFT JOIN first_match m
    ON m.project_id IS NOT DISTINCT FROM b.project_id
   AND m.service_name IS NOT DISTINCT FROM b.service_name
   AND m.app_key = b.app_key
  GROUP BY ALL
),

support AS (
  SELECT month, SUM(net_cost) AS net_cost
  FROM assigned
  WHERE app = '_support'
  GROUP BY month
),

app_share AS (
  SELECT month, app, SUM(net_cost) / NULLIF(SUM(SUM(net_cost)) OVER (PARTITION BY month), 0) AS share
  FROM assigned
  WHERE app <> '_support'
  GROUP BY month, app
)

SELECT date, month, app, service_name, matched_by, app_key, net_cost
FROM assigned
WHERE app <> '_support'

UNION ALL

SELECT
  s.month AS date,
  s.month,
  a.app,
  'Support' AS service_name,
  'support: spread by cost share' AS matched_by,
  NULL AS app_key,
  s.net_cost * a.share AS net_cost
FROM support s
JOIN app_share a USING (month)
