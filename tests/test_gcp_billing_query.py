"""
Tests for how the GCP pipeline reads the billing export (no BigQuery needed).

Run: uv run python -m unittest tests/test_gcp_billing_query.py -v
"""
import pathlib
import sys
import types
import unittest
from unittest import mock

import pyarrow as pa
from google.cloud.bigquery import SchemaField

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipelines"))

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


class FakeJob:
    def __init__(self, batches=None, error=None):
        self.batches, self.error = batches or [], error

    def result(self):
        return self

    def to_arrow_iterable(self, bqstorage_client=None):
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

    def test_falls_back_before_reading_any_row(self):
        with self.patch_storage(), mock.patch("google.cloud.bigquery_storage", fake_storage_module(), create=True):
            rows = gcp._storage_read_rows(FakeJob(error=PermissionError("readsessions.create")), credentials=None)
        self.assertIsNone(rows)

    def test_falls_back_when_package_missing(self):
        with mock.patch.dict(sys.modules, {"google.cloud.bigquery_storage": None}):
            self.assertIsNone(gcp._storage_read_rows(FakeJob(), credentials=None))


if __name__ == "__main__":
    unittest.main()
