"""
Tests for how the GCP pipeline reads the billing export (no BigQuery needed).

Run: uv run python -m unittest tests/test_gcp_billing_query.py -v
"""
import datetime as dt
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest
from unittest import mock

import dlt
import pyarrow as pa
import pyarrow.parquet as pq
from dlt.common import pendulum
from google.cloud.bigquery import SchemaField, TimePartitioning

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipelines"))

import gcp_storage  # noqa: E402
import google_bq_incremental_pipeline as gcp  # noqa: E402

KV = [SchemaField("key", "STRING"), SchemaField("value", "STRING")]
# The parts of the billing export schema that matter here.
SCHEMA = [
    SchemaField("billing_account_id", "STRING"),
    SchemaField("service", "RECORD", fields=[SchemaField("id", "STRING"), SchemaField("description", "STRING")]),
    SchemaField("project", "RECORD", fields=[
        SchemaField("id", "STRING"),
        SchemaField("number", "STRING"),
        SchemaField("name", "STRING"),
        SchemaField("labels", "RECORD", mode="REPEATED", fields=KV),
        SchemaField("ancestry_numbers", "STRING"),
        SchemaField("ancestors", "RECORD", mode="REPEATED",
                    fields=[SchemaField("resource_name", "STRING"), SchemaField("display_name", "STRING")]),
    ]),
    SchemaField("labels", "RECORD", mode="REPEATED", fields=KV),
    SchemaField("system_labels", "RECORD", mode="REPEATED", fields=KV),
    SchemaField("tags", "RECORD", mode="REPEATED", fields=KV),
    SchemaField("cost", "FLOAT"),
    SchemaField("credits", "RECORD", mode="REPEATED",
                fields=[SchemaField("name", "STRING"), SchemaField("amount", "FLOAT")]),
    SchemaField("export_time", "TIMESTAMP"),
]


class BillingSelectListTest(unittest.TestCase):
    def test_keeps_used_columns_and_drops_unused_arrays(self):
        sql = gcp.billing_select_list(SCHEMA, ["k8s-namespace", "app"])
        for column in ("`billing_account_id`", "`service`", "`cost`", "`credits`", "`export_time`"):
            self.assertIn(column, sql)
        self.assertNotIn("system_labels", sql)
        self.assertNotIn("tags", sql)

    def test_project_keeps_scalars_only(self):
        sql = gcp.billing_select_list(SCHEMA, ["app"])
        self.assertIn("STRUCT(project.`id` AS `id`, project.`number` AS `number`, project.`name` AS `name`, "
                      "project.`ancestry_numbers` AS `ancestry_numbers`) AS project", sql)
        self.assertNotIn("ancestors", sql)
        self.assertNotIn("project.`labels`", sql)

    def test_labels_filtered_to_keys(self):
        sql = gcp.billing_select_list(SCHEMA, ["k8s-namespace", "app"])
        self.assertIn("ARRAY(SELECT AS STRUCT l.* FROM UNNEST(labels) AS l "
                      "WHERE l.key IN ('k8s-namespace', 'app')) AS labels", sql)

    def test_empty_label_keys_keep_all_labels(self):
        sql = gcp.billing_select_list(SCHEMA, [])
        self.assertIn("`labels`", sql)
        self.assertNotIn("UNNEST(labels)", sql)

    def test_label_keys_are_quoted(self):
        sql = gcp.billing_select_list(SCHEMA, ["it's"])
        self.assertIn("l.key IN ('it\\'s')", sql)

    def test_standard_export_without_optional_columns(self):
        # Older/standard exports may lack tags or system_labels; nothing breaks.
        schema = [f for f in SCHEMA if f.name not in ("tags", "system_labels")]
        sql = gcp.billing_select_list(schema, gcp.DEFAULT_LABEL_KEYS)
        self.assertIn("`cost`", sql)


class BillingWhereClauseTest(unittest.TestCase):
    def test_incremental_scans_days_from_cursor(self):
        where = gcp.billing_where_clause(TimePartitioning(type_="DAY"), ranged=False)
        self.assertEqual(where, "export_time > @start AND _PARTITIONTIME >= TIMESTAMP_TRUNC(@start, DAY)")

    def test_range_scans_only_its_days(self):
        where = gcp.billing_where_clause(TimePartitioning(type_="DAY"), ranged=True)
        self.assertEqual(where, "export_time >= @start AND export_time < @end"
                                " AND _PARTITIONTIME >= TIMESTAMP_TRUNC(@start, DAY)"
                                " AND _PARTITIONTIME <= TIMESTAMP_TRUNC(@end, DAY)")

    def test_no_partition_filter_without_ingestion_partitions(self):
        # Unpartitioned, or partitioned on a column: _PARTITIONTIME does not exist.
        self.assertEqual(gcp.billing_where_clause(None, ranged=False), "export_time > @start")
        column_partitioned = TimePartitioning(type_="DAY", field="usage_start_time")
        self.assertNotIn("_PARTITIONTIME", gcp.billing_where_clause(column_partitioned, ranged=True))


def utc(*args):
    return dt.datetime(*args, tzinfo=dt.timezone.utc)


class LoadRangeTest(unittest.TestCase):
    def setUp(self):
        self.store = gcp_storage.Store(pathlib.Path(tempfile.mkdtemp()) / "gcp_costs")

    def write(self, month, times):
        """A day file whose rows were exported at `times` (only the export_time columns matter here)."""
        path = self.store.root / "daily" / month / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        times = pa.array(times, pa.timestamp("us", tz="UTC"))
        pq.write_table(pa.table({"first_export_time": times, "export_time": times}), path)

    def test_loaded_range_over_all_files(self):
        self.write("2026-09", [utc(2026, 9, 16, 1), utc(2026, 9, 20)])
        self.write("2026-10", [utc(2026, 10, 19, 3)])
        self.assertEqual(self.store.export_time_range(), (utc(2026, 9, 16, 1), utc(2026, 10, 19, 3)))

    def test_range_before_loaded_rows_is_allowed(self):
        self.write("2026-09", [utc(2026, 9, 16, 1), utc(2026, 10, 19, 3)])
        gcp.check_load_range(self.store, pendulum.datetime(2026, 8, 22), pendulum.datetime(2026, 9, 16))

    def test_overlapping_range_is_refused(self):
        self.write("2026-09", [utc(2026, 9, 16, 1), utc(2026, 10, 19, 3)])
        for start, end in [((2026, 8, 22), (2026, 9, 17)), ((2026, 10, 1), (2026, 10, 2)), ((2026, 10, 19), (2026, 11, 1))]:
            with self.subTest(start=start, end=end), self.assertRaises(SystemExit):
                gcp.check_load_range(self.store, pendulum.datetime(*start), pendulum.datetime(*end))

    def test_refused_without_loaded_rows_or_with_empty_range(self):
        with self.assertRaises(SystemExit):
            gcp.check_load_range(self.store, pendulum.datetime(2026, 8, 22), pendulum.datetime(2026, 9, 16))
        self.write("2026-09", [utc(2026, 9, 16, 1)])
        with self.assertRaises(SystemExit):
            gcp.check_load_range(self.store, pendulum.datetime(2026, 9, 1), pendulum.datetime(2026, 9, 1))


class FakeRestJob:
    """A query job read over REST, as the emulator path reads it."""

    job_id = "job"

    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)


class FakeClient:
    rows, queries = [], []

    def __init__(self, **kwargs):
        pass

    def get_table(self, name):
        return types.SimpleNamespace(schema=[SchemaField("cost", "FLOAT"), SchemaField("export_time", "TIMESTAMP")])

    def query(self, query):
        FakeClient.queries.append(query)
        return FakeRestJob(FakeClient.rows)


class RangeLoadKeepsCursorTest(unittest.TestCase):
    def run_pipeline(self, workdir, rows, **range_args):
        FakeClient.rows = rows
        resource = gcp.bigquery_billing_table(
            "t", dataset="d", project_id="p", initial_start_date="2026-09-16T00:00:00Z",
            api_endpoint="http://emulator", **range_args,
        )
        pipeline = dlt.pipeline("range_test", destination=dlt.destinations.filesystem(f"file://{workdir}/data"),
                                dataset_name="gcp_costs", pipelines_dir=f"{workdir}/pipelines")
        pipeline.run(resource, loader_file_format="parquet")
        state = pipeline.state["sources"]["range_test"]["resources"]["bigquery_billing_table"]
        return state["incremental"]["export_time"]["last_value"]

    def test_range_load_appends_and_leaves_cursor(self):
        workdir = tempfile.mkdtemp()
        with mock.patch.object(gcp.bigquery, "Client", FakeClient):
            cursor = self.run_pipeline(workdir, [{"cost": 1.0, "export_time": utc(2026, 10, 19, 3)}])
            after_range = self.run_pipeline(workdir, [{"cost": 2.0, "export_time": utc(2026, 8, 22, 5)}],
                                            load_start=pendulum.datetime(2026, 8, 22),
                                            load_end=pendulum.datetime(2026, 9, 16))
        self.assertEqual(after_range, cursor)
        self.assertIn("export_time >= TIMESTAMP '2026-08-22 00:00:00.000000'", FakeClient.queries[-1])
        self.assertIn("export_time < TIMESTAMP '2026-09-16 00:00:00.000000'", FakeClient.queries[-1])
        table = pq.ParquetDataset(f"{workdir}/data/gcp_costs/bigquery_billing_table").read(columns=["cost"])
        self.assertEqual(sorted(table.column("cost").to_pylist()), [1.0, 2.0])


class FakeJob:
    def __init__(self, batches=None, error=None):
        self.batches, self.error = batches or [], error
        self.kwargs = None

    def result(self):
        return self

    def to_arrow_iterable(self, bqstorage_client=None, **kwargs):
        self.kwargs = kwargs
        if self.error:
            raise self.error
        return iter(self.batches)


def fake_storage_module():
    module = types.ModuleType("google.cloud.bigquery_storage")
    module.BigQueryReadClient = lambda credentials=None: object()
    return module


class StorageReadRowsTest(unittest.TestCase):
    def patch_storage(self):
        module = fake_storage_module()
        return mock.patch.dict(sys.modules, {"google.cloud.bigquery_storage": module})

    def test_rows_from_arrow_batches(self):
        batches = [pa.record_batch({"cost": [1.5, 2.0], "labels": [[{"key": "app", "value": "a"}], []]}),
                   pa.record_batch({"cost": [3.0], "labels": [[]]})]
        with self.patch_storage(), mock.patch("google.cloud.bigquery_storage", fake_storage_module(), create=True):
            rows = gcp._storage_read_rows(FakeJob(batches), credentials=None)
            self.assertIsNotNone(rows)
            rows = list(rows)
        self.assertEqual([r["cost"] for r in rows], [1.5, 2.0, 3.0])
        self.assertEqual(rows[0]["labels"], [{"key": "app", "value": "a"}])

    def test_limits_streams_and_converts_in_slices(self):
        batch = pa.record_batch({"cost": [float(i) for i in range(2_500)]})
        job = FakeJob([batch])
        with self.patch_storage(), mock.patch("google.cloud.bigquery_storage", fake_storage_module(), create=True):
            rows = list(gcp._storage_read_rows(job, credentials=None))
        self.assertEqual(job.kwargs, {"max_stream_count": gcp._STORAGE_READ_STREAMS})
        self.assertEqual([r["cost"] for r in rows], [float(i) for i in range(2_500)])

    def test_falls_back_before_reading_any_row(self):
        with self.patch_storage(), mock.patch("google.cloud.bigquery_storage", fake_storage_module(), create=True):
            rows = gcp._storage_read_rows(FakeJob(error=PermissionError("readsessions.create")), credentials=None)
        self.assertIsNone(rows)

    def test_falls_back_when_package_missing(self):
        with mock.patch.dict(sys.modules, {"google.cloud.bigquery_storage": None}):
            self.assertIsNone(gcp._storage_read_rows(FakeJob(), credentials=None))


class StorageOnlyJob(FakeJob):
    """Fails like a missing read-session role when read with the Storage Read API, works over REST."""

    def to_arrow_iterable(self, bqstorage_client=None, **kwargs):
        if bqstorage_client is not None:
            raise PermissionError("readsessions.create")
        return iter(self.batches)


class LoadTableToStoreTest(unittest.TestCase):
    def load(self, job, credentials):
        store = gcp_storage.Store(pathlib.Path(tempfile.mkdtemp()) / "gcp_costs")
        with mock.patch.object(gcp, "_start_query", return_value=(job, credentials)), \
                mock.patch.dict(sys.modules, {"google.cloud.bigquery_storage": fake_storage_module()}):
            count = gcp.load_table_to_store(store, "t", "d", "p", None, utc(2026, 10, 1))
        staged = pq.read_table(store.new_staging_file("t"))
        return count, staged, job

    def test_arrow_batches_go_to_a_staging_file(self):
        batches = [pa.record_batch({"cost": [1.0, 2.0], "export_time": pa.array([utc(2026, 10, 2)] * 2)}),
                   pa.record_batch({"cost": [3.0], "export_time": pa.array([utc(2026, 10, 3)])})]
        count, staged, job = self.load(FakeJob(batches), credentials=object())
        self.assertEqual(count, 3)
        self.assertEqual(staged.column("cost").to_pylist(), [1.0, 2.0, 3.0])
        self.assertEqual(job.kwargs, {"max_stream_count": gcp._STORAGE_READ_STREAMS})

    def test_falls_back_to_rest_without_read_session_role(self):
        batches = [pa.record_batch({"cost": [1.0], "export_time": pa.array([utc(2026, 10, 2)])})]
        count, staged, _ = self.load(StorageOnlyJob(batches), credentials=object())
        self.assertEqual((count, staged.column("cost").to_pylist()), (1, [1.0]))


if __name__ == "__main__":
    unittest.main()
