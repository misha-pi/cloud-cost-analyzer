-- AWS's own cost per day for the "AWS cost over time by service" chart (DuckDB mode only): the 10 services
-- with the most net cost over all loaded months, the rest as "Other". Marketplace products (e.g. Vercel) are
-- left out; the AWS page shows them in their own KPI.
WITH top_services AS (
  SELECT service
  FROM {{ ref "aws_by_account" }}
  WHERE provider = 'AWS'
  GROUP BY service
  ORDER BY SUM(net_cost) DESC
  LIMIT 10
)

SELECT
  a.date,
  COALESCE(t.service, 'Other') AS service,
  SUM(a.net_cost) AS net_cost
FROM {{ ref "aws_by_account" }} a
LEFT JOIN top_services t ON t.service = a.service
WHERE a.provider = 'AWS'
GROUP BY ALL
