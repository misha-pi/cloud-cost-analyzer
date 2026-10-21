-- @materialize: true
-- Spike signals for the Cost Spikes page (DuckDB mode only): one row per provider, service and
-- project / cluster (component), all as of the last complete day (as_of_date, from cost_spike_daily):
-- that day vs the 7 days before it, the last 7 days vs the 7 days before, and month to date vs last month
-- up to the same day of the month (the last day of a shorter month).
--
-- charge_pattern marks once-a-month charges (e.g. GCP Support): billed on 2 to 4 of the last 90 days, all
-- on the same day of the month. They would show as a spike every month, so the day and week measures of
-- cost_spike_metrics leave them out; month to date keeps them.
-- latest_loaded_day is the newest day loaded for the provider, to spot a provider whose load stopped.
WITH as_of AS (
  SELECT MAX(date) AS as_of_date FROM {{ ref "cost_spike_daily" }}
),

latest_loaded AS (
  SELECT provider, MAX(date) AS latest_loaded_day FROM {{ ref "cloud_provider_costs" }} GROUP BY provider
),

daily AS (
  SELECT c.provider, c.service, c.component, c.date, a.as_of_date, SUM(c.cost) AS cost
  FROM {{ ref "cloud_provider_costs" }} c
  CROSS JOIN as_of a
  WHERE c.date > a.as_of_date - 90 AND c.date <= a.as_of_date
  GROUP BY ALL
),

signals AS (
  SELECT
    provider,
    service,
    component,
    as_of_date,
    COALESCE(SUM(cost) FILTER (WHERE date = as_of_date), 0) AS latest_day_cost,
    COALESCE(SUM(cost) FILTER (WHERE date BETWEEN as_of_date - 7 AND as_of_date - 1), 0) / 7 AS prev_7d_avg,
    COALESCE(SUM(cost) FILTER (WHERE date BETWEEN as_of_date - 6 AND as_of_date), 0) AS last_7d_cost,
    COALESCE(SUM(cost) FILTER (WHERE date BETWEEN as_of_date - 13 AND as_of_date - 7), 0) AS prev_7d_cost,
    COALESCE(SUM(cost) FILTER (WHERE date >= DATE_TRUNC('month', as_of_date)), 0) AS mtd_cost,
    COALESCE(SUM(cost) FILTER (WHERE date >= DATE_TRUNC('month', as_of_date) - INTERVAL 1 MONTH
                                 AND date <= as_of_date - INTERVAL 1 MONTH), 0) AS prev_mtd_cost,
    COUNT(DISTINCT date) FILTER (WHERE cost <> 0) AS billed_days,
    COUNT(DISTINCT DAY(date)) FILTER (WHERE cost <> 0) AS billed_days_of_month
  FROM daily
  GROUP BY ALL
)

SELECT
  s.provider,
  s.service,
  s.component,
  CASE WHEN s.billed_days BETWEEN 2 AND 4 AND s.billed_days_of_month = 1 THEN 'Monthly charge'
       ELSE 'Daily usage' END AS charge_pattern,
  s.as_of_date,
  l.latest_loaded_day,
  s.latest_day_cost,
  s.prev_7d_avg,
  s.last_7d_cost,
  s.prev_7d_cost,
  s.mtd_cost,
  s.prev_mtd_cost
FROM signals s
JOIN latest_loaded l USING (provider)
