-- @materialize: true
-- Spike signals for the Cost Spikes page (DuckDB mode only): one row per provider, service and
-- project / cluster (component), all as of the last complete day (as_of_date, from cost_spike_daily):
-- that day vs the 7 days before it, the last 7 days vs the 7 days before, and month to date vs last month
-- up to the same day of the month (the last day of a shorter month).
-- The *_period columns spell out the days each number covers, for the page text (same on every row).
--
-- charge_pattern marks once-a-month charges (e.g. GCP Support): billed on 2 to 4 of the last 90 days, all
-- on the same day of the month. They would show as a spike every month, so the day and 7-day measures of
-- cost_spike_metrics leave them out; month to date keeps them.
-- latest_loaded_day is the newest day loaded for the provider, to spot a provider whose load stopped.
WITH as_of AS (
  SELECT
    as_of_date,
    CAST(DATE_TRUNC('month', as_of_date) AS DATE) AS month_start,
    -- Same day last month, or its last day when last month is shorter (Mar 31 -> Feb 28)
    CAST(as_of_date - INTERVAL 1 MONTH AS DATE) AS prev_month_same_day
  FROM (SELECT MAX(date) AS as_of_date FROM {{ ref "cost_spike_daily" }})
),

latest_loaded AS (
  SELECT provider, MAX(date) AS latest_loaded_day FROM {{ ref "cloud_provider_costs" }} GROUP BY provider
),

daily AS (
  SELECT c.provider, c.service, c.component, c.date, a.as_of_date, a.month_start, a.prev_month_same_day,
    SUM(c.cost) AS cost
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
    month_start,
    prev_month_same_day,
    COALESCE(SUM(cost) FILTER (WHERE date = as_of_date), 0) AS latest_day_cost,
    COALESCE(SUM(cost) FILTER (WHERE date BETWEEN as_of_date - 7 AND as_of_date - 1), 0) / 7 AS prev_7d_avg,
    COALESCE(SUM(cost) FILTER (WHERE date BETWEEN as_of_date - 6 AND as_of_date), 0) AS last_7d_cost,
    COALESCE(SUM(cost) FILTER (WHERE date BETWEEN as_of_date - 13 AND as_of_date - 7), 0) AS prev_7d_cost,
    COALESCE(SUM(cost) FILTER (WHERE date >= month_start), 0) AS mtd_cost,
    COALESCE(SUM(cost) FILTER (WHERE date >= month_start - INTERVAL 1 MONTH
                                 AND date <= prev_month_same_day), 0) AS prev_mtd_cost,
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
  s.prev_mtd_cost,
  -- Days in each month-to-date period, to compare cost per day when last month is shorter
  DAY(s.as_of_date) AS mtd_days,
  DAY(s.prev_month_same_day) AS prev_mtd_days,
  STRFTIME(s.as_of_date, '%b %-d') AS latest_day_period,
  STRFTIME(s.as_of_date - 7, '%b %-d') || ' – ' || STRFTIME(s.as_of_date - 1, '%b %-d') AS avg_7d_period,
  STRFTIME(s.as_of_date - 6, '%b %-d') || ' – ' || STRFTIME(s.as_of_date, '%b %-d') AS last_7d_period,
  STRFTIME(s.as_of_date - 13, '%b %-d') || ' – ' || STRFTIME(s.as_of_date - 7, '%b %-d') AS prev_7d_period,
  STRFTIME(s.month_start, '%b %-d') || ' – ' || STRFTIME(s.as_of_date, '%b %-d') AS mtd_period,
  STRFTIME(s.month_start - INTERVAL 1 MONTH, '%b %-d') || ' – ' || STRFTIME(s.prev_month_same_day, '%b %-d')
    AS prev_mtd_period
FROM signals s
JOIN latest_loaded l USING (provider)
