-- MongoDB Atlas invoice headers from the local parquet files (DuckDB mode only).
-- A large invoice is stored as several documents with the same invoice_id and header
-- (segment 0, 1, ...), so keep one row per invoice.
SELECT * FROM read_parquet('data/atlas_billing/invoices/*.parquet', union_by_name = true)
QUALIFY ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY segment) = 1
