# flake8: noqa
import humanize
from typing import Any
import os
import pathlib

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
from dlt.common import pendulum

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from google.oauth2 import service_account


def ensure_credits_table(dataset_name: str) -> None:
    """
    The Rill GCP model joins the credits child table, which dlt only creates once a
    loaded row has credits. Write an empty placeholder so the model always finds a file.
    """
    bucket_url = dlt.config["destination.filesystem.bucket_url"]
    credits_dir = pathlib.Path(bucket_url) / dataset_name / "bigquery_billing_table__credits"
    if any(credits_dir.glob("*.parquet")):
        return
    credits_dir.mkdir(parents=True, exist_ok=True)
    empty = pa.table({
        "_dlt_parent_id": pa.array([], pa.string()),
        "amount": pa.array([], pa.float64()),
    })
    pq.write_table(empty, credits_dir / "_empty.parquet")


def ensure_labels_table(dataset_name: str) -> None:
    """
    The Rill labels model reads the labels child table, which dlt only creates once a
    loaded row has labels. Write an empty placeholder so the model always finds a file.
    """
    bucket_url = dlt.config["destination.filesystem.bucket_url"]
    labels_dir = pathlib.Path(bucket_url) / dataset_name / "bigquery_billing_table__labels"
    if any(labels_dir.glob("*.parquet")):
        return
    labels_dir.mkdir(parents=True, exist_ok=True)
    empty = pa.table({
        "_dlt_parent_id": pa.array([], pa.string()),
        "key": pa.array([], pa.string()),
        "value": pa.array([], pa.string()),
    })
    pq.write_table(empty, labels_dir / "_empty.parquet")


# Print a line this often while reading BigQuery. The first row is always reported.
_PROGRESS_EVERY_ROWS = 10_000


def _utc_timestamp_literal(value) -> str:
    """Format a cursor value as a UTC timestamp literal for the BigQuery emulator."""
    ts = pendulum.parse(value) if isinstance(value, str) else pendulum.instance(value)
    return ts.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")


def bigquery_billing_table(
    table_name: str,
    dataset: str = None,
    project_id: str = None,
    initial_start_date: str = None,
    api_endpoint: str = None,
):
    """
    Load a BigQuery billing table incrementally using export_time as cursor
    """
    # Set up incremental loading with initial start date from config
    if initial_start_date:
        initial_value = pendulum.parse(initial_start_date)
    else:
        initial_value = pendulum.parse("2000-01-01T00:00:00Z")

    # Create the dlt resource with incremental loading decorator
    @dlt.resource(write_disposition="append")
    def _load_table(
        incremental: dlt.sources.incremental[str] = dlt.sources.incremental("export_time", initial_value=initial_value)
    ):
        # Get the last loaded value for incremental loading
        last_value = incremental.last_value
        print(
            f"Submitting query for {table_name} (export_time > {last_value})...",
            flush=True,
        )

        if api_endpoint:
            # Local BigQuery emulator: no service account, and query parameters are unreliable.
            client = bigquery.Client(
                project=project_id,
                client_options=ClientOptions(api_endpoint=api_endpoint),
                credentials=AnonymousCredentials(),
            )
            ts = _utc_timestamp_literal(last_value)
            query = f"""
                SELECT * FROM `{project_id}.{dataset}.{table_name}`
                WHERE export_time > TIMESTAMP '{ts}'
                ORDER BY export_time
            """
            query_job = client.query(query)
        else:
            # Get service account credentials from .dlt/secrets.toml
            service_account_info = {
                "project_id": dlt.secrets.get('source.bigquery.credentials.project_id'),
                "private_key": dlt.secrets.get('source.bigquery.credentials.private_key'),
                "client_email": dlt.secrets.get('source.bigquery.credentials.client_email'),
                "token_uri": dlt.secrets.get('source.bigquery.credentials.token_uri'),
            }
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            client = bigquery.Client(credentials=credentials, project=project_id)

            query = f"""
                SELECT * FROM `{project_id}.{dataset}.{table_name}`
                WHERE export_time > @last_value
                ORDER BY export_time
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("last_value", "TIMESTAMP", last_value)
                ]
            )
            query_job = client.query(query, job_config=job_config)

        print(
            f"BigQuery job {query_job.job_id} started; waiting for the first page of rows...",
            flush=True,
        )
        row_count = 0
        for row in query_job:
            row_count += 1
            if row_count == 1 or row_count % _PROGRESS_EVERY_ROWS == 0:
                print(
                    f"Read {row_count:,} rows from {table_name}; latest export_time={row['export_time']}",
                    flush=True,
                )
            yield {key: value for key, value in row.items()}
        print(f"Finished reading {row_count:,} rows from {table_name}", flush=True)

    # Set the resource name to 'bigquery_billing_table' to maintain consistent output directory
    return _load_table.with_name("bigquery_billing_table")


def load_standalone_table_resource() -> None:
    """Load BigQuery billing export tables with environment-driven destination"""

    # Determine destination from environment variable (default: filesystem for local dev)
    destination = os.getenv("DLT_DESTINATION", "filesystem")

    # Load configuration from config.toml
    try:
        pipeline_name = dlt.config["pipeline.pipeline_name"]
    except KeyError:
        pipeline_name = "cloud_cost_analytics"

    try:
        dataset_name = dlt.config["sources.gcp_billing.dataset_name"]
    except KeyError:
        dataset_name = "gcp_costs"

    # Get initial start date from config (optional)
    try:
        initial_start_date = dlt.config["sources.gcp_billing.initial_start_date"]
    except KeyError:
        initial_start_date = None

    # GCP_BILLING_API_ENDPOINT selects the local emulator. Unset, this is real BigQuery.
    api_endpoint = os.getenv("GCP_BILLING_API_ENDPOINT") or None
    if api_endpoint:
        project_id = os.getenv("GCP_BILLING_PROJECT", "billing-dev")
        dataset = os.getenv("GCP_BILLING_DATASET", "billing")
        tables_raw = os.getenv(
            "GCP_BILLING_TABLES",
            "gcp_billing_export_v1_FAKE00_000000_000000",
        )
        table_names = [name.strip() for name in tables_raw.split(",") if name.strip()]
    else:
        project_id = os.getenv("GCP_BILLING_PROJECT") or dlt.secrets.get('source.bigquery.credentials.project_id')
        dataset = os.getenv("GCP_BILLING_DATASET") or dlt.config["sources.gcp_billing.dataset"]
        tables_raw = os.getenv("GCP_BILLING_TABLES")
        if tables_raw:
            table_names = [name.strip() for name in tables_raw.split(",") if name.strip()]
        else:
            table_names = dlt.config["sources.gcp_billing.table_names"]

    # Create pipeline with environment-driven destination
    # Local: destination="filesystem" writes parquet to viz_rill/data/
    # Production: destination="clickhouse" writes directly to ClickHouse Cloud
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
        # export_schema_path="exported_schema/google_cost_schema.json"
    )

    # Create resources for each table with initial start date
    resources = [bigquery_billing_table(table_name, dataset=dataset, project_id=project_id, initial_start_date=initial_start_date, api_endpoint=api_endpoint) for table_name in table_names]

    # Run the pipeline with incremental (append) write disposition
    # This will only load new records based on export_time
    # Use loader_file_format="parquet" in run() to generate parquet files
    info = pipeline.run(resources, loader_file_format="parquet")
    if destination == "filesystem":
        ensure_credits_table(dataset_name)
        ensure_labels_table(dataset_name)

    # Print concise summary
    print(f"Pipeline {pipeline.pipeline_name} load step completed in {info}")
    print(f"Successfully loaded {len(table_names)} tables to {pipeline.destination} (incremental)")



if __name__ == "__main__":
    # Load selected tables with different settings
    # load_select_tables_from_database()

    # load_entire_database()
    # select_with_end_value_and_row_order()

    # Load tables with the standalone table resource
    load_standalone_table_resource()

    # Load all tables from the database.
    # Warning: The sample database is very large
    # load_entire_database()
