.DEFAULT_GOAL := run-all

check-secrets:
	@if [ ! -f .dlt/secrets.toml ]; then \
		echo ""; \
		echo "================================================================================"; \
		echo "ERROR: Missing .dlt/secrets.toml"; \
		echo "================================================================================"; \
		echo ""; \
		echo "Please create .dlt/secrets.toml from the example:"; \
		echo "  cp .dlt/secrets.toml.example .dlt/secrets.toml"; \
		echo ""; \
		echo "Then edit .dlt/secrets.toml and add your credentials."; \
		echo ""; \
		echo "================================================================================"; \
		echo ""; \
		exit 1; \
	fi

install-rill:
	@if command -v rill >/dev/null 2>&1; then \
		echo "✅ Rill is already installed (version: $$(rill version 2>/dev/null || echo 'unknown'))"; \
	else \
		echo ""; \
		echo "================================================================================"; \
		echo "Rill is not installed"; \
		echo "================================================================================"; \
		echo ""; \
		if [ ! -t 0 ]; then \
			echo "⚠️  Non-interactive mode detected - skipping installation"; \
			echo "Please install Rill manually: curl https://rill.sh | sh"; \
			exit 1; \
		fi; \
		read -p "Install Rill now? (y/N): " answer; \
		if [ "$$answer" = "y" ] || [ "$$answer" = "Y" ]; then \
			curl -fsSL https://rill.sh | sh; \
			echo ""; \
			echo "⚠️  Rill installed. You may need to update your PATH:"; \
			echo "   export PATH=\"\$$HOME/.rill:\$$PATH\""; \
			echo ""; \
		else \
			echo "Skipped. Install later with: curl https://rill.sh | sh"; \
			exit 1; \
		fi; \
	fi

install: install-rill
	mkdir -p viz_rill/data/
	@if [ ! -f viz_rill/.env ]; then \
		echo "📋 Copying viz_rill/.env.example to viz_rill/.env"; \
		cp viz_rill/.env.example viz_rill/.env; \
		echo "✅ Created viz_rill/.env (you can edit it later if needed)"; \
	else \
		echo "✅ viz_rill/.env already exists"; \
	fi
	uv sync

dlt-clear:
	# rm -rf ~/.dlt/
	rm -rf ~/.local/share/dlt/

clear-data:
	@if [ -f cloud_cost_analytics.duckdb ]; then \
		mv cloud_cost_analytics.duckdb cloud_cost_analytics.bak_$(shell date +%Y%m%d_%H%M%S).duckdb; \
	fi
	rm -rf viz_rill/data
	mkdir -p viz_rill/data

clear-rill:
	@echo "Clearing Rill cache and materialized views..."
	rm -rf viz_rill/tmp/
	@echo "✅ Rill cache cleared"

clear-clickhouse:
	@echo "Clearing ClickHouse tables (interactive)..."
	uv run python scripts/clear_clickhouse.py

clear-clickhouse-force:
	@echo "⚠️  Force clearing ClickHouse tables (non-interactive)..."
	@echo "yes" | uv run python scripts/clear_clickhouse.py

clear: dlt-clear clear-data clear-rill

## Per-source cleanup: delete one source's local data (viz_rill/data/<dataset>) so its next load reloads
## it in full; other sources are untouched. Shows what it would delete; add CONFIRM=1 to delete.
## New source: add a clear-<source> line with its dataset folder and its run target.
clear-source:
	@sh scripts/clear_source.sh "$(SOURCE)" "$(RELOAD)"

clear-gcp:
	@sh scripts/clear_source.sh gcp_costs run-gcp

clear-atlas:
	@sh scripts/clear_source.sh atlas_billing run-atlas

clear-all: clear clear-clickhouse

## Connector switching helpers
# BSD sed (macOS) requires a suffix for -i; GNU sed accepts -i.bak too.
SED_INPLACE = sed -i.bak

setup-connector-duckdb:
	@$(SED_INPLACE) 's/^olap_connector:.*$$/olap_connector: duckdb/' viz_rill/rill.yaml && rm -f viz_rill/rill.yaml.bak
	@test -f viz_rill/.env || cp viz_rill/.env.example viz_rill/.env
	@$(SED_INPLACE) 's/^RILL_CONNECTOR=.*$$/RILL_CONNECTOR=""/' viz_rill/.env && rm -f viz_rill/.env.bak

setup-connector-clickhouse:
	@$(SED_INPLACE) 's/^olap_connector:.*$$/olap_connector: clickhouse/' viz_rill/rill.yaml && rm -f viz_rill/rill.yaml.bak
	@test -f viz_rill/.env || cp viz_rill/.env.example viz_rill/.env
	@$(SED_INPLACE) 's/^RILL_CONNECTOR=.*$$/RILL_CONNECTOR="clickhouse"/' viz_rill/.env && rm -f viz_rill/.env.bak

setup-connector-motherduck:
	@$(SED_INPLACE) 's/^olap_connector:.*$$/olap_connector: motherduck/' viz_rill/rill.yaml && rm -f viz_rill/rill.yaml.bak
	@test -f viz_rill/.env || cp viz_rill/.env.example viz_rill/.env
	@$(SED_INPLACE) 's/^RILL_CONNECTOR=.*$$/RILL_CONNECTOR="motherduck"/' viz_rill/.env && rm -f viz_rill/.env.bak

set-connector-duckdb:
	@echo "Setting Rill connector to DuckDB..."
	@$(MAKE) setup-connector-duckdb
	@echo "✅ Connector set to DuckDB (local parquet files)"

set-connector-clickhouse:
	@echo "Setting Rill connector to ClickHouse..."
	@$(MAKE) setup-connector-clickhouse
	@echo "✅ Connector set to ClickHouse (cloud database)"

set-connector-motherduck:
	@echo "Setting Rill connector to MotherDuck..."
	@$(MAKE) setup-connector-motherduck
	@echo "✅ Connector set to MotherDuck (cloud DuckDB)"


run-aws: check-secrets
	uv run python pipelines/aws_pipeline.py
	echo "####################################################################"
run-gcp: check-secrets
	uv run python pipelines/google_bq_incremental_pipeline.py
	echo "####################################################################"
	@[ "$(SKIP_RILL_REFRESH)" = "1" ] || $(MAKE) --no-print-directory rill-refresh
# Local BigQuery emulator (no .dlt/secrets.toml). Start the emulator first.
run-gcp-emulator:
	GCP_BILLING_API_ENDPOINT=http://localhost:9050 \
	GCP_BILLING_PROJECT=billing-dev \
	GCP_BILLING_DATASET=billing \
	GCP_BILLING_TABLES=gcp_billing_export_v1_FAKE00_000000_000000 \
	uv run python pipelines/google_bq_incremental_pipeline.py
	echo "####################################################################"
run-stripe: check-secrets
	uv run python pipelines/stripe_pipeline.py
	echo "####################################################################"


run-atlas: check-secrets
	uv run python pipelines/mongodb_atlas_billing_pipeline.py
	uv run python scripts/generate_atlas_forecast_page.py
	echo "####################################################################"
	@[ "$(SKIP_RILL_REFRESH)" = "1" ] || $(MAKE) --no-print-directory rill-refresh
# Made-up Atlas data in viz_rill/data/atlas_billing (gitignored), no .dlt/secrets.toml needed.
# `make demo` runs this; run it alone to see the Atlas pages next to your own GCP data.
atlas-demo-data:
	PYTHON="uv run python" sh scripts/atlas_demo_data.sh
	uv run python scripts/generate_atlas_forecast_page.py

#run dlt incremental loads
# Pipelines we run. Add new sources here (upstream also ran run-aws and run-stripe).
# Rill is refreshed once at the end, not after each source.
run-etl: check-secrets
	@$(MAKE) --no-print-directory run-gcp SKIP_RILL_REFRESH=1
	@$(MAKE) --no-print-directory run-atlas SKIP_RILL_REFRESH=1
	@$(MAKE) --no-print-directory rill-refresh

# Rebuild Rill's models from newly loaded files. Rill does not notice new parquet files by itself, the
# GCP models are materialized (stored as tables), and models that failed while their files were missing
# are not retried. Harmless when Rill is not running: it reads the new data when it starts.
RILL_URL ?= http://localhost:9009
rill-refresh:
	@curl -fs -m 10 -X POST "$(RILL_URL)/v1/instances/default/trigger" \
		-H 'content-type: application/json' -d '{"all": true}' >/dev/null \
		&& echo "Rill at $(RILL_URL) is rebuilding its models; the dashboards update in a few minutes." \
		|| echo "Rill is not running at $(RILL_URL); it will read the new data when it starts (make serve)."

# Daily load on the server: adds scripts/daily_load.sh to root's crontab (02:00 UTC), replacing an older
# line for it. Its output is in logs/daily_load.log.
CRON_TIME ?= 0 2 * * *
install-cron:
	@( crontab -l 2>/dev/null | grep -v 'scripts/daily_load.sh' ; \
	   echo "$(CRON_TIME) $(CURDIR)/scripts/daily_load.sh" ) | crontab -
	@crontab -l | grep daily_load

# One-off: move GCP data loaded before the day/month files (one file per load, labels and credits in
# child tables) into them. Stop Rill first on the server. Old files are kept unless DELETE_OLD=1.
migrate-gcp-storage:
	uv run python scripts/migrate_gcp_storage.py viz_rill/data/gcp_costs $(if $(filter 1,$(DELETE_OLD)),--delete-old)

test-atlas:
	uv run python -m unittest discover -s tests -p "test_atlas*.py" -v


test-duplicates-duckdb:
	@echo "Running duplicate checks on cloud_cost_analytics.duckdb..."
	@duckdb cloud_cost_analytics.duckdb < tests/test_duplicates.sql

test-duplicates:
	@echo "Running duplicate checks on parquet files in viz_rill/data..."
	@duckdb < tests/test_duplicates_parquet.sql

test: test-duplicates

rill-deploy:
	rill deploy \
	--org demo \
	--path viz_rill \
	--public \
	--prod-branch main \

# The Atlas forecast page is generated from the data (one chart per project); write it before starting.
serve:
	uv run python scripts/generate_atlas_forecast_page.py
	rill start viz_rill --pull-env=false

serve-duckdb: setup-connector-duckdb
	@echo "Starting Rill with DuckDB connector..."
	@rill start viz_rill || true
	@$(MAKE) setup-connector-duckdb

serve-clickhouse: setup-connector-clickhouse
	@echo "Starting Rill with ClickHouse connector..."
	@rill start viz_rill || true
	@$(MAKE) setup-connector-clickhouse

demo: install-rill setup-connector-duckdb
	@echo "================================================================================"
	@echo "Running in DEMO mode with sample data"
	@echo "================================================================================"
	@echo ""
	@sh scripts/check_no_real_data.sh viz_rill/data/*/
	@echo "Clearing existing data directory..."
	@rm -rf viz_rill/data
	@mkdir -p viz_rill/data
	@echo "Copying demo data to viz_rill/data/..."
	@cp -r viz_rill/data_demo/* viz_rill/data/
	@for d in viz_rill/data/*/; do touch "$${d}_demo_data"; done
	@echo "✅ Demo data copied successfully"
	@PYTHON="uv run python" sh scripts/atlas_demo_data.sh
	@uv run python scripts/generate_atlas_forecast_page.py
	@echo "✅ Connector set to DuckDB (local parquet files)"
	@echo ""
	@echo "Starting Rill dashboards with demo data..."
	@echo "NOTE: Run 'make clear' before running 'make run-all' to use real data"
	@echo ""
	@$(MAKE) setup-connector-duckdb
	@rill start viz_rill || true
	@$(MAKE) setup-connector-duckdb

## AWS Advanced Analytics (CUR Wizard integration)
aws-normalize:
	@echo "Normalizing AWS CUR data..."
	cd viz_rill && uv run python cur-wizard/scripts/normalize.py

aws-generate-dashboards:
	@echo "Generating AWS-specific Rill dashboards..."
	cd viz_rill && uv run python cur-wizard/scripts/generate_rill_yaml.py \
		--parquet data/normalized_aws.parquet \
		--output-dir . \
		--cost-col line_item_unblended_cost \
		--dim-prefixes "product_,line_item_" \
		--timeseries-col date

aws-dashboards: aws-normalize aws-generate-dashboards
	@echo "✅ AWS dashboards generated! Run 'make serve' to view them."

## GCP Advanced Analytics (CUR Wizard integration)
gcp-normalize:
	@echo "Normalizing GCP billing data..."
	cd viz_rill && uv run python cur-wizard/scripts/normalize_gcp.py

gcp-generate-dashboards:
	@echo "Generating GCP-specific Rill dashboards..."
	cd viz_rill && uv run python cur-wizard/scripts/generate_gcp_rill_yaml.py \
		--parquet data/normalized_gcp.parquet \
		--output-dir . \
		--cost-col cost \
		--dim-prefixes "labels_,service__,project__" \
		--timeseries-col date

# Optional, run by hand: creates the "GCP Labels Analysis" pages (one list per label, service and project field).
# make run-all does not run it (see the note above run-all). It does not change gcp_overview.yaml.
gcp-dashboards: gcp-normalize gcp-generate-dashboards
	@echo "✅ GCP dashboards generated! Run 'make serve' to view them."

#what this does:
# 1. load data incrementally
# 2. normalizes AWS cost reports and generates the AWS Rill dashboards
# 3. starts Rill BI and opens in browser
# The GCP pages are written by hand (gcp_overview, gcp_overview_service_app, gcp_atlas_canvas, summary).
# gcp-dashboards is left out on purpose: it would bring back the "GCP Labels Analysis" pages removed in PI-18090.
run-all: install run-etl aws-dashboards serve-duckdb



## Production / ClickHouse (writes directly to ClickHouse Cloud)
run-aws-clickhouse:
	DLT_DESTINATION=clickhouse uv run python pipelines/aws_pipeline.py
	echo "####################################################################"
run-gcp-clickhouse:
	DLT_DESTINATION=clickhouse uv run python pipelines/google_bq_incremental_pipeline.py
	echo "####################################################################"
run-stripe-clickhouse:
	DLT_DESTINATION=clickhouse uv run python pipelines/stripe_pipeline.py
	echo "####################################################################"

# Run dlt incremental loads (production - clickhouse destination)
run-etl-clickhouse: run-aws-clickhouse run-gcp-clickhouse run-stripe-clickhouse
	@echo "✅ ClickHouse ETL complete (data in ClickHouse Cloud)"

# Initialize ClickHouse database (run once before first use)
init-clickhouse:
	@echo "Initializing ClickHouse database..."
	uv run python scripts/init_clickhouse.py

# Ingest normalized data to ClickHouse
ingest-normalized-clickhouse:
	@echo "Ingesting normalized AWS & GCP data to ClickHouse..."
	DLT_DESTINATION=clickhouse uv run python pipelines/ingest_normalized_pipeline.py

## MotherDuck (writes directly to MotherDuck cloud DuckDB)
run-aws-motherduck:
	DLT_DESTINATION=motherduck uv run python pipelines/aws_pipeline.py
	echo "####################################################################"
run-gcp-motherduck:
	DLT_DESTINATION=motherduck uv run python pipelines/google_bq_incremental_pipeline.py
	echo "####################################################################"
run-stripe-motherduck:
	DLT_DESTINATION=motherduck uv run python pipelines/stripe_pipeline.py
	echo "####################################################################"

# Run dlt incremental loads (MotherDuck destination)
run-etl-motherduck: run-aws-motherduck run-gcp-motherduck run-stripe-motherduck
	@echo "✅ MotherDuck ETL complete (data in MotherDuck cloud)"

serve-motherduck: setup-connector-motherduck
	@echo "Starting Rill with MotherDuck connector..."
	@rill start viz_rill || true
	@$(MAKE) setup-connector-motherduck

clear-motherduck:
	@echo "Clearing MotherDuck schemas (interactive)..."
	uv run python scripts/clear_motherduck.py

run-all-motherduck: install run-etl-motherduck serve-motherduck

## Cloud Deployment with Anonymization (for public demos)
# Simple approach: Run normal ETL, then anonymize data directly in ClickHouse
anonymize-clickhouse:
	@echo ""
	@echo "================================================================================"
	@echo "Anonymizing ClickHouse Data for Public Demos"
	@echo "================================================================================"
	@echo ""
	@if [ -f .env ]; then \
		set -a; . ./.env; set +a; \
		uv run python scripts/anonymize_clickhouse.py; \
	else \
		uv run python scripts/anonymize_clickhouse.py; \
	fi
	@echo ""

# Complete cloud pipeline with anonymization
# Note: Dynamic dashboard generation (aws-dashboards/gcp-dashboards) requires local parquet files,
# so it's excluded from cloud mode. Static dashboards work with ClickHouse via models.
run-all-cloud: check-secrets run-etl-clickhouse anonymize-clickhouse serve-clickhouse
	@echo ""
	@echo "================================================================================"
	@echo "✅ Cloud deployment complete with anonymized data!"
	@echo "================================================================================"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Set RILL_CONNECTOR=clickhouse in viz_rill/.env"
	@echo "  2. Run 'make serve' to view dashboards with ClickHouse data"
	@echo "  3. Configure Rill Cloud to connect to your ClickHouse instance"
	@echo ""
	@echo "Useful commands:"
	@echo "  make anonymize-clickhouse     # Re-anonymize data"
	@echo "  make clear-clickhouse         # Drop all ClickHouse tables (interactive)"
	@echo "  make clear-clickhouse-force   # Drop all ClickHouse tables (non-interactive)"
	@echo ""
	@echo "Customize anonymization with environment variables:"
	@echo "  COST_MULTIPLIER_MIN=2.0 COST_MULTIPLIER_MAX=8.0 DUPLICATE_ROWS=3"
	@echo ""
	@echo "================================================================================"

## Docker
# The project .env is for dlt. Compose uses docker/compose.env so it does not
# interpolate those values.
COMPOSE = docker compose --env-file docker/compose.env

# demo: dashboards on bundled sample data at http://localhost:9009
# serve: dashboards on local viz_rill/data
# etl: pipelines write parquet into local viz_rill/data (needs .dlt/secrets.toml)
docker-build:
	$(COMPOSE) build

docker-demo:
	$(COMPOSE) up --build

docker-serve:
	@test -f viz_rill/.env || cp viz_rill/.env.example viz_rill/.env
	@mkdir -p viz_rill/data
	$(COMPOSE) run --rm --service-ports \
		-v "$(CURDIR)/viz_rill/data:/app/viz_rill/data" \
		-v "$(CURDIR)/viz_rill/.env:/app/viz_rill/.env" \
		cloud-cost-analyzer serve

docker-etl:
	@test -f .dlt/secrets.toml || (echo "Missing .dlt/secrets.toml. Copy .dlt/secrets.toml.example and add credentials."; exit 1)
	@mkdir -p viz_rill/data
	$(COMPOSE) run --rm \
		-v "$(CURDIR)/.dlt/secrets.toml:/app/.dlt/secrets.toml:ro" \
		-v "$(CURDIR)/viz_rill/data:/app/viz_rill/data" \
		cloud-cost-analyzer make run-etl
