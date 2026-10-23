-- Monthly MongoDB Atlas cost for the Atlas Spend Forecast page (DuckDB mode only): the total and every
-- project, each with a straight-line trend. One row per month, line and series (the cost, or its trend).
--
-- The 12 months are the 12 complete calendar months before the month of the latest billed day (that
-- month is still being billed). Each closed month adds up to its invoice subtotal.
-- Projects are ranked by their cost over those months; org-level lines (Support, ...) have no project and
-- are one line, "(no project)".
WITH bounds AS (
  SELECT CAST(date_trunc('month', MAX(date)) AS DATE) AS open_month
  FROM {{ ref "atlas_costs" }}
),

months AS (
  SELECT CAST(s.month AS DATE) AS month, CAST(ROW_NUMBER() OVER (ORDER BY s.month) - 1 AS INTEGER) AS month_index
  FROM bounds b, generate_series(b.open_month - INTERVAL 12 MONTH, b.open_month - INTERVAL 1 MONTH, INTERVAL 1 MONTH) AS s(month)
  -- Only months the data covers (fewer than 12 when the history is shorter)
  WHERE s.month >= (SELECT date_trunc('month', MIN(date)) FROM {{ ref "atlas_costs" }})
),

project_months AS (
  SELECT m.month, c.project_name AS project, SUM(c.cost) AS cost
  FROM {{ ref "atlas_costs" }} c
  JOIN months m ON CAST(date_trunc('month', c.date) AS DATE) = m.month
  GROUP BY ALL
),

projects AS (
  SELECT project, ROW_NUMBER() OVER (ORDER BY SUM(cost) DESC) AS project_rank
  FROM project_months
  GROUP BY project
),

-- Every line has a row for every month (0 when nothing was billed), so the trend is fitted on all months.
line_months AS (
  SELECT 0 AS project_rank, 'All MongoDB Atlas' AS project, m.month, m.month_index, COALESCE(SUM(p.cost), 0) AS cost
  FROM months m
  LEFT JOIN project_months p USING (month)
  GROUP BY ALL
  UNION ALL
  SELECT r.project_rank, r.project, m.month, m.month_index, COALESCE(p.cost, 0) AS cost
  FROM projects r
  CROSS JOIN months m
  LEFT JOIN project_months p ON p.project = r.project AND p.month = m.month
),

trend AS (
  SELECT project_rank, REGR_INTERCEPT(cost, month_index) AS intercept, REGR_SLOPE(cost, month_index) AS slope
  FROM line_months
  GROUP BY project_rank
)

SELECT
  l.month,
  l.month_index,
  l.project_rank,
  l.project,
  k.kind,
  -- The legend shows the project on the cost line, so each project chart names its project.
  CASE WHEN k.kind = 'cost' THEN CASE WHEN l.project_rank = 0 THEN l.project ELSE '#' || l.project_rank || ' ' || l.project END
       ELSE 'Straight-line trend' END AS series,
  CASE WHEN k.kind = 'cost' THEN l.cost ELSE t.intercept + t.slope * l.month_index END AS cost
FROM line_months l
JOIN trend t USING (project_rank)
CROSS JOIN (VALUES ('cost'), ('trend')) AS k(kind)
