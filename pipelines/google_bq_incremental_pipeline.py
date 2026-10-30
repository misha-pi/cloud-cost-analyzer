# flake8: noqa
import humanize
from typing import Any
import itertools
import os
import pathlib
import shutil

import dlt
import gcp_storage
import pyarrow.compute as pc
import pyarrow.parquet as pq
from dlt.common import pendulum

from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from google.oauth2 import service_account


def check_load_range(store, load_start, load_end) -> None:
    """
    A date-range load adds rows, so it must not overlap rows already loaded: refuse when
    [load_start, load_end) overlaps the stored rows' export_time range (gaps inside it included).
    """
    if load_start >= load_end:
        raise SystemExit(f"GCP_LOAD_START ({load_start}) must be before GCP_LOAD_END ({load_end})")
    loaded = store.export_time_range()
    if loaded is None:
        raise SystemExit("No local GCP billing data: run `make run-gcp` once before loading a date range")
    earliest, latest = loaded
    if load_start <= latest and load_end > earliest:
        raise SystemExit(
            f"Range {load_start} .. {load_end} overlaps the loaded rows ({earliest} .. {latest}); "
            "loading it would count rows twice. Pick a range before or after them."
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

# Labels kept for the ClickHouse and MotherDuck destinations when sources.gcp_billing.label_keys is not
# set (the local files use gcp_storage.LABEL_COLUMNS). An empty list keeps every label.
DEFAULT_LABEL_KEYS = ["k8s-namespace", "k8s-workload-name", "app"]


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


def _start_query(table_name, dataset, project_id, api_endpoint, select_list, start, end=None):
    """
    Start the billing query for export_time > start (or start <= export_time < end) with the SELECT list
    `select_list(schema)` returns. Returns (query_job, credentials); credentials is None for the emulator.
    """
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
            SELECT {select_list(table.schema)}
            FROM `{project_id}.{dataset}.{table_name}`
            WHERE {where}
        """
        return client.query(query), None

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
        SELECT {select_list(table.schema)}
        FROM `{project_id}.{dataset}.{table_name}`
        WHERE {billing_where_clause(table.time_partitioning, ranged)}
    """
    query_parameters = [bigquery.ScalarQueryParameter("start", "TIMESTAMP", start)]
    if ranged:
        query_parameters.append(bigquery.ScalarQueryParameter("end", "TIMESTAMP", end))
    job_config = bigquery.QueryJobConfig(use_query_cache=False, query_parameters=query_parameters)
    query_job = client.query(query, job_config=job_config)
    print(f"BigQuery job {query_job.job_id} started; waiting for the result...", flush=True)
    return query_job, credentials


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
    dlt resource for the ClickHouse and MotherDuck destinations: load a BigQuery billing table
    incrementally using export_time as cursor.
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
        query_job, credentials = _start_query(
            table_name, dataset, project_id, api_endpoint,
            lambda schema: billing_select_list(schema, label_keys), start, end,
        )
        # The emulator has no usable Storage Read API: read over REST.
        rows = _storage_read_rows(query_job, credentials) if credentials is not None else None
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


def _arrow_batches(query_job, credentials):
    """
    The query result as Arrow batches: over the Storage Read API when it is available, else over REST
    (the emulator, a missing package or a missing "BigQuery Read Session User" role).
    """
    if credentials is not None:
        try:
            from google.cloud import bigquery_storage
            read_client = bigquery_storage.BigQueryReadClient(credentials=credentials)
            batches = iter(query_job.result().to_arrow_iterable(
                bqstorage_client=read_client, max_stream_count=_STORAGE_READ_STREAMS,
            ))
            first = next(batches, None)
            print(f"Reading rows with the BigQuery Storage Read API ({_STORAGE_READ_STREAMS} streams)", flush=True)
            return itertools.chain([first] if first is not None else [], batches)
        except Exception as exc:  # ImportError, or PermissionDenied on bigquery.readsessions.create
            print(f"BigQuery Storage Read API unavailable ({type(exc).__name__}: {exc}); reading rows over REST", flush=True)
    return iter(query_job.result().to_arrow_iterable())


def load_table_to_store(store, table_name, dataset, project_id, api_endpoint, start, end=None) -> int:
    """
    Filesystem destination: read one billing table's new rows as Arrow, flat (gcp_storage.flat_select_list),
    into a staging file of `store`. Arrow goes to parquet as it is, without turning rows into Python
    objects. Returns the number of rows read.
    """
    query_job, credentials = _start_query(
        table_name, dataset, project_id, api_endpoint, gcp_storage.flat_select_list, start, end,
    )
    row_count = 0
    writer = None
    try:
        for batch in _arrow_batches(query_job, credentials):
            if batch.num_rows == 0:
                continue
            if writer is None:
                writer = pq.ParquetWriter(store.new_staging_file(table_name), batch.schema)
            writer.write_batch(batch)
            previous, row_count = row_count, row_count + batch.num_rows
            if previous == 0 or previous // _PROGRESS_EVERY_ROWS != row_count // _PROGRESS_EVERY_ROWS:
                latest = pc.max(batch.column("export_time")).as_py()
                print(f"Read {row_count:,} rows from {table_name}; latest export_time={latest}", flush=True)
    finally:
        if writer is not None:
            writer.close()
    print(f"Finished reading {row_count:,} rows from {table_name}", flush=True)
    return row_count


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

    # Label keys to keep for ClickHouse and MotherDuck (an empty list keeps all labels).
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

    # GCP_LOAD_START and GCP_LOAD_END (export_time, end excluded) load only that range, e.g. days that
    # are still in BigQuery but older than the loaded rows. The daily cursor is left as it is.
    load_start, load_end = os.getenv("GCP_LOAD_START") or None, os.getenv("GCP_LOAD_END") or None
    if bool(load_start) != bool(load_end):
        raise SystemExit("Set both GCP_LOAD_START and GCP_LOAD_END, or neither")
    if load_start:
        if destination != "filesystem":
            raise SystemExit("GCP_LOAD_START/GCP_LOAD_END only work with the filesystem destination")
        load_start, load_end = pendulum.parse(load_start), pendulum.parse(load_end)

    if destination == "filesystem":
        # Local files for Rill: day and month files (see pipelines/gcp_storage.py), written without dlt.
        remove_demo_data(dataset_name)
        store = gcp_storage.Store(pathlib.Path(dlt.config["destination.filesystem.bucket_url"]) / dataset_name)
        store.clear_work()
        if load_start:
            check_load_range(store, load_start, load_end)
            start, end = load_start, load_end
        else:
            loaded = store.export_time_range()
            start = loaded[1] if loaded else pendulum.parse(initial_start_date or "2000-01-01T00:00:00Z")
            end = None
            if not loaded:
                print(f"No local GCP billing data: loading everything from {start}", flush=True)
        for table_name in table_names:
            load_table_to_store(store, table_name, dataset, project_id, api_endpoint, start, end)
        added = store.add()
        mode = f"range {load_start} .. {load_end}" if load_start else f"export_time > {start}"
        print(f"Added {added:,} GCP billing rows to {store.root} ({mode})")
        return

    # ClickHouse and MotherDuck: dlt writes the billing rows as they are.
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
    )
    resources = [bigquery_billing_table(table_name, dataset=dataset, project_id=project_id, initial_start_date=initial_start_date, api_endpoint=api_endpoint, label_keys=label_keys) for table_name in table_names]
    info = pipeline.run(resources, loader_file_format="parquet")

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
