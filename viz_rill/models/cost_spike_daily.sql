-- @materialize: true
-- Daily cost for the Cost Spikes page (DuckDB mode only): one row per day, provider and line (`series`).
-- Built from cloud_provider_costs, so a new provider shows up here once it has a block there.
--
-- It stops at the last complete day (as_of_date). The newest day of each provider is still being filled in
-- (the GCP export runs about a day behind and keeps adding rows and credits), and the combined line must
-- not end in a fake drop when one provider is a day behind; so as_of_date is the day before the newest day
-- of the provider that is furthest behind.
--
-- series is what the per-provider charts draw: the provider's top 5 of the last 30 days, the rest as
-- "Other". Atlas spend changes per cluster, so Atlas uses project / cluster; other providers use service.
--
-- cost_7d_avg is the average of the 7 days up to and including the day, worked out here because Rill
-- window measures only add up the rows a chart loads. Every series has a row for every day (0 when
-- nothing was billed), so the averages of several series add up to the average of their total.
WITH daily AS (
  SELECT
    date,
    provider,
    CASE WHEN provider = 'MongoDB Atlas' THEN component ELSE service END AS series_key,
    SUM(cost) AS cost
  FROM {{ ref "cloud_provider_costs" }}
  GROUP BY ALL
),

as_of AS (
  SELECT MIN(latest_day) - 1 AS as_of_date
  FROM (SELECT provider, MAX(date) AS latest_day FROM daily GROUP BY provider)
),

top_series AS (
  SELECT d.provider, d.series_key
  FROM daily d
  CROSS JOIN as_of a
  WHERE d.date > a.as_of_date - 30 AND d.date <= a.as_of_date
  GROUP BY d.provider, d.series_key
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.provider ORDER BY SUM(d.cost) DESC) <= 5
),

grouped AS (
  SELECT d.date, d.provider, COALESCE(t.series_key, 'Other') AS series, SUM(d.cost) AS cost
  FROM daily d
  LEFT JOIN top_series t ON t.provider = d.provider AND t.series_key = d.series_key
  CROSS JOIN as_of a
  WHERE d.date <= a.as_of_date
  GROUP BY ALL
),

-- Every day from the provider's first day to as_of_date, for every series of the provider.
spine AS (
  SELECT s.provider, s.series, CAST(g.day AS DATE) AS date
  FROM (SELECT provider, series, MIN(MIN(date)) OVER (PARTITION BY provider) AS first_day
        FROM grouped GROUP BY provider, series) s
  CROSS JOIN as_of a
  CROSS JOIN LATERAL GENERATE_SERIES(s.first_day, a.as_of_date, INTERVAL 1 DAY) AS g(day)
)

SELECT
  s.date,
  s.provider,
  s.series,
  COALESCE(g.cost, 0) AS cost,
  AVG(COALESCE(g.cost, 0)) OVER (
    PARTITION BY s.provider, s.series ORDER BY s.date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS cost_7d_avg
FROM spine s
LEFT JOIN grouped g ON g.provider = s.provider AND g.series = s.series AND g.date = s.date
