-- MongoDB Atlas billing line items from the local parquet files (DuckDB mode only), loaded by
-- pipelines/mongodb_atlas_billing_pipeline.py from atlas_billing.lineItems.
-- Kept as its own single-source model so Rill resolves the data/ path.
SELECT * FROM read_parquet('data/atlas_billing/line_items/*.parquet', union_by_name = true)
