# flake8: noqa
import humanize
from typing import Any
import itertools
import os
import pathlib
import shutil

import dlt
import pyarrow as pa
import pyarrow.compute as pc
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


def billing_files_missing(dataset_name: str) -> bool:
    """
    True when the local billing table has no parquet files, e.g. after `make demo` or `make clear-data`
    wiped viz_rill/data. dlt keeps the incremental cursor (last export_time) in ~/.dlt, so without a
    reset it would only load rows newer than the old cursor and the wiped history would stay missing.
    """
    bucket_url = dlt.config["destination.filesystem.bucket_url"]
    table_dir = pathlib.Path(bucket_url) / dataset_name / "bigquery_billing_table"
    return not any(table_dir.glob("*.parquet"))


def loaded_export_time_range(dataset_name: str):
    """
    (earliest, latest) export_time in the local billing files, or None when there are none.
    Reads only the export_time column, one file at a time.
    """
    bucket_url = dlt.config["destination.filesystem.bucket_url"]
    table_dir = pathlib.Path(bucket_url) / dataset_name / "bigquery_billing_table"
    earliest = latest = None
    for path in table_dir.glob("*.parquet"):
        column = pq.read_table(path, columns=["export_time"]).column("export_time")
        bounds = pc.min_max(column).as_py()
        if bounds["min"] is None:
            continue
        earliest = bounds["min"] if earliest is None else min(earliest, bounds["min"])
        latest = bounds["max"] if latest is None else max(latest, bounds["max"])
    return None if earliest is None else (earliest, latest)


def check_load_range(dataset_name: str, load_start, load_end) -> None:
    """
    A date-range load appends rows, so it must not overlap rows already loaded: refuse when
    [load_start, load_end) overlaps the local files' export_time range (gaps inside it included).
    """
    if load_start >= load_end:
        raise SystemExit(f"GCP_LOAD_START ({load_start}) must be before GCP_LOAD_END ({load_end})")
    loaded = loaded_export_time_range(dataset_name)
    if loaded is None:
        raise SystemExit("No local GCP billing files: run `make run-gcp` once before loading a date range")
    earliest, latest = loaded
    if load_start <= latest and load_end > earliest:
        raise SystemExit(
            f"Range {load_start} .. {load_end} overlaps the loaded rows ({earliest} .. {latest}); "
            "loading it would duplicate rows. Pick a range before or after them."
        )
    print(f"Loaded rows: {earliest} .. {latest}; loading {load_start} .. {load_end} (end excluded)", flush=True)


def remove_demo_data(dataset_name: str) -> None:
    """
    `make demo` marks its data with a `_demo_data` file. Delete such a folder before a real load, so
    real rows are not appended to demo rows (the result would look like real data).
    """
    bucket_url = dlt.config["destination.filesystem.bucket_url"]
    dataset_dir = pathlib.Path(bucket_url) / dataset_name
    if (dataset_dir / "_demo_data").exists():
        print(f"Removing demo data in {dataset_dir} before loading real data", flush=True)
        shutil.rmtree(dataset_dir)


# Print a line this often while reading BigQuery. The first row is always reported.
_PROGRESS_EVERY_ROWS = 10_000

# Parallel Storage Read API streams. Each stream's download thread holds a page and the client queues
# about one more page per stream, so memory grows with the stream count. With BigQuery's default
# (it picks the count) a 16M-row reload reached 3.6 GB and was killed on the 4 GB server. Converting
# rows to Python, not the download, limits the speed, so a few streams are enough.
_STORAGE_READ_STREAMS = 2

# Arrow rows turned into Python dicts at a time. Converting a whole batch at once holds all of its
# rows as dicts in memory.
_ROWS_PER_SLICE = 1_000


# Arrays the dashboards do not use. dlt turns every array element into a row of a child table, so
# these made up most of the rows written (e.g. 1.5 months: 47M labels, 17.8M project ancestors).
_DROPPED_COLUMNS = {"system_labels", "tags"}

# Labels kept when sources.gcp_billing.label_keys is not set: the two the Rill models read
# (viz_rill/models/gcp_labels_per_row.sql). An empty list keeps every label.
DEFAULT_LABEL_KEYS = ["k8s-namespace", "app"]


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def billing_select_list(schema, label_keys) -> str:
    """
    SELECT list for the billing export: every column except the arrays the dashboards do not use.
    - system_labels and tags are dropped;
    - project keeps its scalar fields (id, number, name, ...) but not its ancestors and labels arrays;
    - labels keeps only `label_keys` (all labels when empty).
    Built from the table's schema, so it works for the standard and the resource-level export.
    """
    columns = []
    for field in schema:
        name = field.name
        if name in _DROPPED_COLUMNS:
            continue
        if name == "project" and field.field_type in ("RECORD", "STRUCT"):
            kept = [f.name for f in field.fields if f.mode != "REPEATED"]
            columns.append("STRUCT(" + ", ".join(f"project.`{f}` AS `{f}`" for f in kept) + ") AS project")
        elif name == "labels" and field.mode == "REPEATED" and label_keys:
            keys = ", ".join(_sql_string(k) for k in label_keys)
            columns.append(f"ARRAY(SELECT AS STRUCT l.* FROM UNNEST(labels) AS l WHERE l.key IN ({keys})) AS labels")
        else:
            columns.append(f"`{name}`")
    return ",\n                  ".join(columns)


def _storage_read_rows(query_job, credentials):
    """
    Read the query result with the BigQuery Storage Read API: Arrow batches over parallel streams,
    much faster than the REST pages the plain iterator fetches. Returns None, before any row is read,
    when it is not available (package not installed, or the service account lacks the
    "BigQuery Read Session User" role), so the caller can fall back to REST without duplicates.
    """
    try:
        from google.cloud import bigquery_storage
    except ImportError:
        print("google-cloud-bigquery-storage is not installed; reading rows over REST", flush=True)
        return None
    try:
        read_client = bigquery_storage.BigQueryReadClient(credentials=credentials)
        batches = iter(query_job.result().to_arrow_iterable(
            bqstorage_client=read_client, max_stream_count=_STORAGE_READ_STREAMS,
        ))
        first = next(batches, None)
    except Exception as exc:  # e.g. PermissionDenied on bigquery.readsessions.create
        print(f"BigQuery Storage Read API unavailable ({type(exc).__name__}: {exc}); reading rows over REST", flush=True)
        return None

    def rows():
        # to_pylist gives the same Python values as the REST rows: dicts for STRUCTs, lists for
        # ARRAYs, timezone-aware datetimes, Decimal for NUMERIC.
        for batch in itertools.chain([first] if first is not None else [], batches):
            for offset in range(0, batch.num_rows, _ROWS_PER_SLICE):
                yield from batch.slice(offset, _ROWS_PER_SLICE).to_pylist()

    print(f"Reading rows with the BigQuery Storage Read API ({_STORAGE_READ_STREAMS} streams)", flush=True)
    return rows()


def _utc_timestamp_literal(value) -> str:
    """Format a cursor value as a UTC timestamp literal for the BigQuery emulator."""
    ts = pendulum.parse(value) if isinstance(value, str) else pendulum.instance(value)
    return ts.in_timezone("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")


def billing_where_clause(time_partitioning, ranged: bool) -> str:
    """
    WHERE clause on export_time: `> @start` for the incremental load, `>= @start AND < @end` for a
    date-range load. The billing export is partitioned by ingestion day, and every row's partition day
    is its export_time day (checked on the resource-level export), so the same bounds on _PARTITIONTIME
    make BigQuery scan only those days instead of the whole table.
    """
    conditions = ["export_time >= @start", "export_time < @end"] if ranged else ["export_time > @start"]
    if time_partitioning is not None and time_partitioning.field is None:  # ingestion-time partitions
        conditions.append("_PARTITIONTIME >= TIMESTAMP_TRUNC(@start, DAY)")
        if ranged:
            conditions.append("_PARTITIONTIME <= TIMESTAMP_TRUNC(@end, DAY)")
    return " AND ".join(conditions)


def bigquery_billing_table(
    table_name: str,
    dataset: str = None,
    project_id: str = None,
    initial_start_date: str = None,
    api_endpoint: str = None,
    label_keys: list = None,
    load_start=None,
    load_end=None,
):
    """
    Load a BigQuery billing table incrementally using export_time as cursor.
    With load_start and load_end, load only that export_time range instead (start included, end
    excluded) and leave the saved cursor as it is.
    Only the columns and labels the dashboards use are read (see billing_select_list).
    """
    if label_keys is None:
        label_keys = DEFAULT_LABEL_KEYS
    # Set up incremental loading with initial start date from config
    if initial_start_date:
        initial_value = pendulum.parse(initial_start_date)
    else:
        initial_value = pendulum.parse("2000-01-01T00:00:00Z")

    def _read_rows(start, end=None):
        ranged = end is not None
        bounds = f"{start} <= export_time < {end}" if ranged else f"export_time > {start}"
        print(f"Submitting query for {table_name} ({bounds})...", flush=True)

        if api_endpoint:
            # Local BigQuery emulator: no service account, and query parameters are unreliable.
            client = bigquery.Client(
                project=project_id,
                client_options=ClientOptions(api_endpoint=api_endpoint),
                credentials=AnonymousCredentials(),
            )
            table = client.get_table(f"{project_id}.{dataset}.{table_name}")
            where = billing_where_clause(None, ranged)
            where = where.replace("@start", f"TIMESTAMP '{_utc_timestamp_literal(start)}'")
            if ranged:
                where = where.replace("@end", f"TIMESTAMP '{_utc_timestamp_literal(end)}'")
            query = f"""
                SELECT {billing_select_list(table.schema, label_keys)}
                FROM `{project_id}.{dataset}.{table_name}`
                WHERE {where}
            """
            query_job = client.query(query)
            rows = None  # the emulator has no usable Storage Read API: read over REST
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

            table = client.get_table(f"{project_id}.{dataset}.{table_name}")
            query = f"""
                SELECT {billing_select_list(table.schema, label_keys)}
                FROM `{project_id}.{dataset}.{table_name}`
                WHERE {billing_where_clause(table.time_partitioning, ranged)}
            """

            query_parameters = [bigquery.ScalarQueryParameter("start", "TIMESTAMP", start)]
            if ranged:
                query_parameters.append(bigquery.ScalarQueryParameter("end", "TIMESTAMP", end))
            job_config = bigquery.QueryJobConfig(use_query_cache=False, query_parameters=query_parameters)
            query_job = client.query(query, job_config=job_config)
            print(f"BigQuery job {query_job.job_id} started; waiting for the result...", flush=True)
            rows = _storage_read_rows(query_job, credentials)

        if rows is None:
            print(
                f"BigQuery job {query_job.job_id} started; waiting for the first page of rows...",
                flush=True,
            )
            rows = ({key: value for key, value in row.items()} for row in query_job)
        row_count = 0
        for row in rows:
            row_count += 1
            if row_count == 1 or row_count % _PROGRESS_EVERY_ROWS == 0:
                print(
                    f"Read {row_count:,} rows from {table_name}; latest export_time={row['export_time']}",
                    flush=True,
                )
            yield row
        print(f"Finished reading {row_count:,} rows from {table_name}", flush=True)

    # Create the dlt resource with incremental loading decorator
    @dlt.resource(write_disposition="append")
    def _load_table(
        incremental: dlt.sources.incremental[str] = dlt.sources.incremental("export_time", initial_value=initial_value)
    ):
        # Get the last loaded value for incremental loading
        yield from _read_rows(incremental.last_value)

    # A date-range load has no incremental, so dlt keeps the saved cursor unchanged.
    @dlt.resource(write_disposition="append")
    def _load_range():
        yield from _read_rows(load_start, load_end)

    # Set the resource name to 'bigquery_billing_table' to maintain consistent output directory
    resource = _load_range if load_start is not None else _load_table
    return resource.with_name("bigquery_billing_table")


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

    # Label keys to keep (an empty list keeps all labels, e.g. for the cur-wizard label dashboards).
    label_keys = dlt.config.get("sources.gcp_billing.label_keys")
    if label_keys is None:
        label_keys = DEFAULT_LABEL_KEYS

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

    # GCP_LOAD_START and GCP_LOAD_END (export_time, end excluded) load only that range, e.g. days that
    # are still in BigQuery but older than the loaded rows. The daily cursor is left as it is.
    load_start, load_end = os.getenv("GCP_LOAD_START") or None, os.getenv("GCP_LOAD_END") or None
    if bool(load_start) != bool(load_end):
        raise SystemExit("Set both GCP_LOAD_START and GCP_LOAD_END, or neither")
    if load_start:
        if destination != "filesystem":
            raise SystemExit("GCP_LOAD_START/GCP_LOAD_END only work with the filesystem destination")
        load_start, load_end = pendulum.parse(load_start), pendulum.parse(load_end)

    # Create resources for each table with initial start date
    resources = [bigquery_billing_table(table_name, dataset=dataset, project_id=project_id, initial_start_date=initial_start_date, api_endpoint=api_endpoint, label_keys=label_keys, load_start=load_start, load_end=load_end) for table_name in table_names]

    # Run the pipeline with incremental (append) write disposition
    # This will only load new records based on export_time
    # Use loader_file_format="parquet" in run() to generate parquet files
    # After the local files were wiped, drop the saved cursor and reload from initial_start_date.
    refresh = None
    if destination == "filesystem":
        remove_demo_data(dataset_name)
    if load_start:
        check_load_range(dataset_name, load_start, load_end)
    elif destination == "filesystem" and billing_files_missing(dataset_name):
        print("No local GCP billing files: reloading everything from initial_start_date", flush=True)
        refresh = "drop_sources"
    info = pipeline.run(resources, loader_file_format="parquet", refresh=refresh)
    if destination == "filesystem":
        ensure_credits_table(dataset_name)
        ensure_labels_table(dataset_name)

    # Print concise summary
    print(f"Pipeline {pipeline.pipeline_name} load step completed in {info}")
    mode = f"range {load_start} .. {load_end}" if load_start else "incremental"
    print(f"Successfully loaded {len(table_names)} tables to {pipeline.destination} ({mode})")



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
