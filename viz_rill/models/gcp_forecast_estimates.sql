-- 12-month GCP cost estimates for the GCP Spend Forecast page (DuckDB mode only): one row for the total
-- (sku_rank 0) and one per top-20 SKU, from the complete weeks in gcp_forecast_weekly.
--
-- Past 12 months (estimate): average weekly net cost x 52. Only about 8 weeks of billing data exist, so
-- this assumes the rest of the year cost what these weeks cost.
-- Next 12 months (projection): the past-12-month estimate x 1.1 (10% growth allowance, agreed for the
-- contract talks).
SELECT
  sku_rank,
  CAST(sku_rank AS VARCHAR) AS rank_text,
  sku,
  CASE WHEN sku_rank = 0 THEN 'total' ELSE 'sku' END AS line_type,
  strftime(MIN(week), '%Y-%m-%d') || ' to ' || strftime(MAX(week) + 6, '%Y-%m-%d') AS weeks_used,
  COUNT(*) AS week_count,
  AVG(cost) AS avg_weekly_cost,
  REGR_SLOPE(cost, week_index) AS weekly_trend,
  AVG(cost) * 52 AS past_12m_estimate,
  AVG(cost) * 52 * 1.1 AS next_12m_projection
FROM {{ ref "gcp_forecast_weekly" }}
WHERE kind = 'cost'
GROUP BY sku_rank, sku
