-- GCP net cost per app (DuckDB mode only): every billing row of gcp_costs gets an app from gcp_app_rules.
-- Net cost is after credits, like billing-helper's sheet. Feeds the "GCP Cost by Service and App" page,
-- which replaces the monthly sheet built with billing-helper.
--
-- Support is billed for the whole account, so (like billing-helper) each month's support cost is
-- spread over the apps by their share of that month's net cost, as "Support" rows dated on the 1st.
-- matched_by says which rule assigned the row ("unmatched" rows go to "other"), to explain differences.
WITH billing_rows AS (
  SELECT
    c._dlt_id AS row_id,
    c.date,
    CAST(DATE_TRUNC('month', c.date) AS DATE) AS month,
    c.project_id,
    c.service_name,
    c.net_cost,
    LOWER(COALESCE(l.k8s_namespace, l.app_label, c.resource__name, '')) AS app_key
  FROM {{ ref "gcp_costs" }} c
  LEFT JOIN {{ ref "gcp_labels_per_row" }} l ON l._dlt_parent_id = c._dlt_id
),

first_match AS (
  SELECT
    b.row_id,
    ARG_MIN(r.app, r.priority) AS app,
    ARG_MIN(r.match_on || ': ' || r.pattern, r.priority) AS matched_by
  FROM billing_rows b
  JOIN {{ ref "gcp_app_rules" }} r
    ON (r.match_on = 'project' AND LOWER(b.project_id) LIKE r.pattern)
    OR (r.match_on = 'service' AND LOWER(b.service_name) LIKE r.pattern)
    OR (r.match_on = 'name' AND b.app_key LIKE r.pattern)
  GROUP BY b.row_id
),

assigned AS (
  SELECT
    b.*,
    COALESCE(m.app, 'other') AS app,
    COALESCE(m.matched_by, 'unmatched') AS matched_by
  FROM billing_rows b
  LEFT JOIN first_match m USING (row_id)
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
