-- @materialize: true
-- Materialized: stored once as a DuckDB table (rebuilt when the data is refreshed, see `make rill-refresh`)
-- instead of re-reading the GCP parquet on every dashboard query.
-- GCP Costs Model
-- Switches between DuckDB (parquet), MotherDuck, and ClickHouse based on RILL_CONNECTOR env var
-- Selects ALL columns + derived columns to maintain compatibility with existing dashboards

{{ if eq .env.RILL_CONNECTOR "clickhouse" }}
-- ClickHouse: Query table directly
SELECT
  toDate(usage_start_time) AS date,
  service__description AS service_name,
  sku__description AS sku_description,
  location__location AS region,
  location__country AS country,
  project__id AS project_id,

  project__name AS project_name,
  *
FROM gcp_costs___bigquery_billing_table
WHERE cost IS NOT NULL

{{ else if eq .env.RILL_CONNECTOR "motherduck" }}
-- MotherDuck: Query table in cloud DuckDB (same SQL syntax as local DuckDB)
SELECT
  CAST(usage_start_time AS DATE) AS date,
  service__description AS service_name,
  sku__description AS sku_description,
  location__location AS region,
  location__country AS country,
  project__id AS project_id,
  project__name AS project_name,
  *
FROM gcp_costs.bigquery_billing_table
WHERE cost IS NOT NULL

{{ else }}
-- DuckDB: Read from parquet files (default for local development)
-- Billing rows come from gcp_billing_raw and per-row credit sums from gcp_credits_per_row.
-- Each file read lives in its own simple model: Rill only resolves the relative
-- data/ paths for single-source queries, not inside CTEs or joins. `ref` makes Rill
-- build those two models first (it cannot infer dependencies from templated SQL).
SELECT
  CAST(b.usage_start_time AS DATE) AS date,
  b.service__description AS service_name,
  b.sku__description AS sku_description,
  b.location__location AS region,
  b.location__country AS country,
  b.project__id AS project_id,
  b.project__name AS project_name,
  COALESCE(c.credits_total, 0) AS credits_total,
  b.cost + COALESCE(c.credits_total, 0) AS net_cost,
  b.*
FROM {{ ref "gcp_billing_raw" }} b
LEFT JOIN {{ ref "gcp_credits_per_row" }} c ON c._dlt_parent_id = b._dlt_id
WHERE b.cost IS NOT NULL

{{ end }}
