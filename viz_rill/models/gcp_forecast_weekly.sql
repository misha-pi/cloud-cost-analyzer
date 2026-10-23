-- @materialize: true
-- Weekly GCP net cost for the GCP Spend Forecast page (DuckDB mode only): the total and the top 20 SKUs,
-- each with a straight-line trend. One row per week, line and series (the cost, or its trend).
--
-- Only complete Monday-Sunday weeks are used. A usage day is complete from the first export day (rows
-- for earlier usage days only partly arrived before the export history starts) to the day before the
-- latest export day (that day is still being filled in). The billing export keeps about 60 days, so this
-- is about 8 weeks.
--
-- The top 20 SKUs are ranked by net cost over those weeks. A SKU is identified by its SKU id and named
-- "service / SKU description". gcp_costs already leaves out MongoDB Atlas billed through the Marketplace.
WITH bounds AS (
  SELECT
    GREATEST(MIN(date), CAST(MIN(export_time) AS DATE)) AS first_day,
    LEAST(MAX(date), CAST(MAX(export_time) AS DATE) - 1) AS last_day
  FROM {{ ref "gcp_costs" }}
),

weeks AS (
  SELECT CAST(s.week AS DATE) AS week, CAST(ROW_NUMBER() OVER (ORDER BY s.week) - 1 AS INTEGER) AS week_index
  FROM bounds b, generate_series(
    -- first Monday on or after first_day, last Monday whose week ends by last_day
    CAST(date_trunc('week', b.first_day - 1) AS DATE) + 7,
    CAST(date_trunc('week', b.last_day + 1) AS DATE) - 7,
    INTERVAL 7 DAY
  ) AS s(week)
),

sku_weeks AS (
  SELECT
    w.week,
    c.sku__id AS sku_id,
    ANY_VALUE(c.service_name || ' / ' || c.sku_description) AS sku,
    SUM(c.net_cost) AS net_cost
  FROM {{ ref "gcp_costs" }} c
  JOIN weeks w ON CAST(date_trunc('week', c.date) AS DATE) = w.week
  GROUP BY ALL
),

top_skus AS (
  SELECT sku_id, ANY_VALUE(sku) AS sku, ROW_NUMBER() OVER (ORDER BY SUM(net_cost) DESC) AS sku_rank
  FROM sku_weeks
  GROUP BY sku_id
  QUALIFY sku_rank <= 20
),

-- Every line has a row for every week (0 when nothing was billed), so the trend is fitted on all weeks.
line_weeks AS (
  SELECT 0 AS sku_rank, 'All GCP' AS sku, w.week, w.week_index, COALESCE(SUM(s.net_cost), 0) AS net_cost
  FROM weeks w
  LEFT JOIN sku_weeks s USING (week)
  GROUP BY ALL
  UNION ALL
  SELECT t.sku_rank, t.sku, w.week, w.week_index, COALESCE(s.net_cost, 0) AS net_cost
  FROM top_skus t
  CROSS JOIN weeks w
  LEFT JOIN sku_weeks s ON s.sku_id = t.sku_id AND s.week = w.week
),

trend AS (
  SELECT sku_rank, REGR_INTERCEPT(net_cost, week_index) AS intercept, REGR_SLOPE(net_cost, week_index) AS slope
  FROM line_weeks
  GROUP BY sku_rank
)

SELECT
  l.week,
  l.week_index,
  l.sku_rank,
  l.sku,
  k.kind,
  -- The legend shows the SKU on the cost line, so each SKU chart names its SKU.
  CASE WHEN k.kind = 'cost' THEN CASE WHEN l.sku_rank = 0 THEN 'All GCP' ELSE '#' || l.sku_rank || ' ' || l.sku END
       ELSE 'Straight-line trend' END AS series,
  CASE WHEN k.kind = 'cost' THEN l.net_cost ELSE t.intercept + t.slope * l.week_index END AS cost
FROM line_weeks l
JOIN trend t USING (sku_rank)
CROSS JOIN (VALUES ('cost'), ('trend')) AS k(kind)
