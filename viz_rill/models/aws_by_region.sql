-- AWS cost per day, service and region (DuckDB mode only), from the Cost Explorer files
-- written by pipelines/aws_cost_explorer_pipeline.py.
-- provider is "AWS", or "<product> (via AWS)" for Marketplace products billed through AWS (e.g. Vercel).
-- net_cost is after credits and discounts; estimated marks days AWS has not finalised yet.
SELECT * FROM read_parquet('data/aws_costs/by_region/*.parquet', union_by_name = true)
