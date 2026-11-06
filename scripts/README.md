# Scripts

Utility scripts for the cloud cost analyzer project.

## Anonymization

### `anonymize_clickhouse.py`
Anonymizes cost data in ClickHouse for public demos.

**Usage:**
```bash
# Run anonymization on existing ClickHouse data
make anonymize-clickhouse

# Or run directly with custom settings
COST_MULTIPLIER_MIN=5.0 COST_MULTIPLIER_MAX=10.0 uv run python scripts/anonymize_clickhouse.py
```

**What it does:**
- Multiplies all cost values by random factors (default: 2-8x)
- Duplicates rows to generate more data (default: 3x)
- Hashes sensitive identifiers (account IDs, project IDs, customer IDs)

**Environment variables:**
- `COST_MULTIPLIER_MIN` (default: 2.0) - Minimum cost multiplier
- `COST_MULTIPLIER_MAX` (default: 8.0) - Maximum cost multiplier
- `DUPLICATE_ROWS` (default: 3) - Row duplication factor

See [../ANONYMIZATION.md](../ANONYMIZATION.md) for complete anonymization guide.

## Reports

### `gcp_sku_estimate_csv.py`
GCP 12-month cost estimates for every SKU as a CSV: the All GCP row, one row per SKU with non-zero net cost
(ranked by cost) and a total row. Same rules as the GCP Spend Report page (average week x 52, x 1.1 for the
next 12 months, NK suggested = 40%), over all complete days in the daily billing data. Reads `gcp_costs`
from a running Rill.

**Usage (on costgenie, or locally through the SSH tunnel to its Rill):**
```bash
uv run python scripts/gcp_sku_estimate_csv.py --out gcp_sku_estimates.csv
```

## ClickHouse Management

### `init_clickhouse.py`
Initializes ClickHouse database for the project.

**Usage:**
```bash
make init-clickhouse
```

Creates necessary database schemas and users in ClickHouse Cloud.

### `clear_clickhouse.py`
Drops all dlt-created tables from ClickHouse.

**Usage:**
```bash
# Interactive mode (asks for confirmation)
make clear-clickhouse

# Non-interactive mode (for scripts)
make clear-clickhouse-force

# Dry run (see what would be deleted)
uv run python scripts/clear_clickhouse.py --dry-run
```

**What it does:**
- Lists all dlt tables (`aws_costs___*`, `gcp_costs___*`, `stripe_costs___*`, `aws_costs_staging___*`)
- Lists all Rill model tables (`aws_costs`, `gcp_costs`, `stripe_revenue`, `unified_cost_model`)
- Asks for confirmation before dropping (unless using force mode)
- Drops all matching tables
- Safe to run - only drops tables created by our ETL and Rill
