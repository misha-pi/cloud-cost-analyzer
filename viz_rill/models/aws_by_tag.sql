-- AWS cost per day, service and cost allocation tag value, one set per tag key in [sources.aws] tag_keys
-- (DuckDB mode only), from the Cost Explorer files written by pipelines/aws_cost_explorer_pipeline.py.
-- provider is "AWS", or "<product> (via AWS)" for Marketplace products billed through AWS (e.g. Vercel).
-- net_cost is after credits and discounts; estimated marks days AWS has not finalised yet.
-- Cost without the tag is tag_value "(no tag)". Empty until a tag key is active and listed in tag_keys.
SELECT * FROM read_parquet('data/aws_costs/by_tag/*.parquet', union_by_name = true)
