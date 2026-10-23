-- 12-month MongoDB Atlas cost for the Atlas Spend Forecast page (DuckDB mode only): one row for the total
-- (project_rank 0) and one per project, from the complete months in atlas_forecast_monthly.
--
-- Past 12 months (actual): billed cost over the 12 complete months.
-- Next 12 months (projection): past 12 months x 1.1 (10% growth allowance, agreed for the contract talks).
SELECT
  project_rank,
  CAST(project_rank AS VARCHAR) AS rank_text,
  project,
  CASE WHEN project_rank = 0 THEN 'total' ELSE 'project' END AS line_type,
  strftime(MIN(month), '%Y-%m') || ' to ' || strftime(MAX(month), '%Y-%m') AS months_used,
  COUNT(*) AS month_count,
  AVG(cost) AS avg_monthly_cost,
  REGR_SLOPE(cost, month_index) AS monthly_trend,
  SUM(cost) AS past_12m_actual,
  SUM(cost) * 1.1 AS next_12m_projection
FROM {{ ref "atlas_forecast_monthly" }}
WHERE kind = 'cost'
GROUP BY project_rank, project
