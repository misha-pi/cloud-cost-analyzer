-- Unified Cloud Cost Model
-- Combines AWS costs (Cost Explorer) and GCP costs. Stripe revenue was left out: it was never loaded.
-- All amounts converted to USD for consistent reporting
-- @materialize: true

-- Currency conversion rates (update as needed)
-- CHF to USD: 1.13 (as of 2025-11-13)
WITH currency_rates AS (
  SELECT 1.26 AS chf_to_usd
),

-- AWS Costs (already in USD)
aws_cost_data AS (
  -- From Cost Explorer: one row per day, provider, service and region; Marketplace products keep their
  -- own provider (e.g. "Vercel (via AWS)").
  SELECT
    date,
    provider AS cloud_provider,
    service AS service_name,
    region,
    service AS description,
    NULL AS account_id,
    net_cost AS amount,
    'cost' AS transaction_type,
    'USD' AS original_currency,
    net_cost AS amount_usd
  FROM {{ ref "aws_by_region" }}
),

gcp_cost_data AS (
  SELECT
    date,
    'GCP' AS cloud_provider,
    COALESCE(service_name, 'Unknown') AS service_name,
    COALESCE(region, 'global') AS region,
    COALESCE(sku_description, service_name) AS description,
    project_id AS account_id,
    cost AS amount,
    COALESCE(transaction_type, 'cost') AS transaction_type,
    currency AS original_currency,
    -- Convert CHF to USD
    CASE
      WHEN UPPER(currency) = 'CHF' THEN cost * (SELECT chf_to_usd FROM currency_rates)
      WHEN UPPER(currency) = 'USD' THEN cost
      ELSE cost  -- Fallback: use original amount
    END AS amount_usd
  FROM {{ ref "gcp_costs" }}
)

SELECT
  date,
  cloud_provider,
  service_name,
  region,
  description,
  account_id,
  amount AS original_amount,
  original_currency,
  amount_usd,
  transaction_type,
  'USD' AS currency,  -- All amounts normalized to USD
  -- Categorize into cost and revenue for easy analysis (using USD amounts)
  CASE
    WHEN transaction_type IN ('revenue', 'charge') THEN amount_usd
    ELSE 0
  END AS revenue_amount,
  CASE
    WHEN transaction_type IN ('cost', 'fee') THEN ABS(amount_usd)
    WHEN transaction_type NOT IN ('revenue', 'charge') AND amount_usd > 0 THEN amount_usd
    ELSE 0
  END AS cost_amount
FROM (
  SELECT date, cloud_provider, service_name, region, description, account_id,
         amount, transaction_type, original_currency, amount_usd
  FROM aws_cost_data
  UNION ALL
  SELECT date, cloud_provider, service_name, region, description, account_id,
         amount, transaction_type, original_currency, amount_usd
  FROM gcp_cost_data
)
-- No ORDER BY: sorting the ~5.6M rows while building the table runs out of Rill's memory on the server.
WHERE date IS NOT NULL
