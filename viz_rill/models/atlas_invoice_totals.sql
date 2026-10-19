-- One row per MongoDB Atlas invoice (DuckDB mode only): invoice amounts next to the sum of its
-- line items, to check the cluster breakdown adds up to what Atlas bills.
-- A PENDING (open) invoice's subtotal is not filled in until the month closes; use
-- line_items_cost for the month to date. When credits cover an invoice, a PAID
-- invoice has credits = billed and amount_paid = 0.
WITH line_item_totals AS (
  SELECT invoice_id, SUM(cost) AS line_items_cost, COUNT(*) AS line_item_rows
  FROM {{ ref "atlas_costs" }}
  GROUP BY invoice_id
)
SELECT
  i.invoice_id,
  i.start_date AS invoice_date,
  STRFTIME(i.start_date, '%Y-%m') AS invoice_month,
  CAST(YEAR(i.start_date) AS VARCHAR) AS invoice_year,
  i.status,
  CAST(i.subtotal AS DOUBLE) AS subtotal,
  CAST(i.credits AS DOUBLE) AS credits,
  CAST(i.sales_tax AS DOUBLE) AS sales_tax,
  CAST(i.amount_billed AS DOUBLE) AS amount_billed,
  CAST(i.amount_paid AS DOUBLE) AS amount_paid,
  COALESCE(t.line_items_cost, 0) AS line_items_cost,
  COALESCE(t.line_item_rows, 0) AS line_item_rows,
  CASE WHEN i.status = 'PENDING' THEN NULL
       ELSE ROUND(CAST(i.subtotal AS DOUBLE) - COALESCE(t.line_items_cost, 0), 2) END AS unexplained_cost
FROM {{ ref "atlas_invoices_raw" }} i
LEFT JOIN line_item_totals t ON t.invoice_id = i.invoice_id
