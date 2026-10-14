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
-- Works with the standard or the resource-level export: the standard export has no
-- resource columns, so they are added as NULL (the metrics view uses them as dimensions).
-- `cost` is before credits. A row can have several credits (child table), so sum
-- them per row before joining; joining credit rows directly would repeat `cost`.
WITH credits_per_row AS (
  SELECT _dlt_parent_id, SUM(amount) AS credits_total
  FROM read_parquet('data/gcp_costs/bigquery_billing_table__credits/*.parquet', union_by_name = true)
  GROUP BY _dlt_parent_id
)
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
FROM (
  SELECT * FROM read_parquet('data/gcp_costs/bigquery_billing_table/*.parquet')
  UNION ALL BY NAME
  SELECT NULL::VARCHAR AS resource__name, NULL::VARCHAR AS resource__global_name WHERE false
) b
LEFT JOIN credits_per_row c ON c._dlt_parent_id = b._dlt_id
WHERE b.cost IS NOT NULL

{{ end }}
