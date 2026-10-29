"""
Tests for the GCP day/month files (pipelines/gcp_storage.py). No BigQuery needed.

Run: uv run python -m unittest tests/test_gcp_storage.py -v
"""
import datetime as dt
import pathlib
import sys
import tempfile
import unittest

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud.bigquery import SchemaField

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipelines"))

import gcp_storage  # noqa: E402

FLOATS = {"cost", "cost_at_list", "usage__amount", "usage__amount_in_pricing_units", "credits_total"}


def utc(*args):
    return dt.datetime(*args, tzinfo=dt.timezone.utc)


def flat_rows(rows):
    """Flat billing rows as the BigQuery query returns them; unset columns are NULL (credits 0)."""
    names = [n for n, _ in gcp_storage.FIELDS] + list(gcp_storage.LABEL_COLUMNS.values()) + ["credits_total"]
    columns = {}
    for name in names:
        if name in ("usage_start_time", "export_time"):
            kind = pa.timestamp("us", tz="UTC")
        elif name in FLOATS:
            kind = pa.float64()
        else:
            kind = pa.string()
        default = 0.0 if name == "credits_total" else None
        columns[name] = pa.array([r.get(name, default) for r in rows], kind)
    return pa.table(columns)


def row(usage, export, cost, **extra):
    return {"usage_start_time": usage, "export_time": export, "cost": cost, "service__description": "Compute Engine",
            "project__id": "p1", **extra}


class StoreTest(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp()) / "gcp_costs"
        self.store = gcp_storage.Store(self.root)

    def add(self, rows, name="t"):
        pq.write_table(flat_rows(rows), self.store.new_staging_file(name))
        return self.store.add()

    def query(self, sql, files):
        return duckdb.sql(sql.replace("ROWS", f"({gcp_storage._read(files)})")).fetchall()

    def stored(self, sql):
        return self.query(sql, self.store._daily_files() + self.store._monthly_files())

    def test_rows_are_summed_per_day_and_dimensions(self):
        added = self.add([
            row(utc(2026, 10, 1, 1), utc(2026, 10, 1, 5), 1.0, credits_total=-0.5, k8s_workload="api"),
            row(utc(2026, 10, 1, 2), utc(2026, 10, 1, 6), 2.0, k8s_workload="api"),
            row(utc(2026, 10, 1, 2), utc(2026, 10, 1, 6), 4.0, k8s_workload="web"),
            row(utc(2026, 10, 2, 0), utc(2026, 10, 2, 3), 8.0, k8s_workload="api"),
        ])
        self.assertEqual(added, 4)
        self.assertEqual([p.relative_to(self.root).as_posix() for p in self.store._daily_files()],
                         ["daily/2026-10/data.parquet"])
        self.assertEqual(
            self.stored("SELECT grain, date, k8s_workload, cost, credits_total, row_count FROM ROWS ORDER BY date, k8s_workload"),
            [("day", dt.date(2026, 10, 1), "api", 3.0, -0.5, 2),
             ("day", dt.date(2026, 10, 1), "web", 4.0, 0.0, 1),
             ("day", dt.date(2026, 10, 2), "api", 8.0, 0.0, 1)],
        )
        self.assertEqual(self.store.export_time_range(), (utc(2026, 10, 1, 5), utc(2026, 10, 2, 3)))
        self.assertFalse(self.store.work_dir.exists())

    def test_later_loads_merge_into_the_month_file(self):
        self.add([row(utc(2026, 10, 1, 1), utc(2026, 10, 1, 5), 1.0)])
        self.add([row(utc(2026, 10, 1, 9), utc(2026, 10, 2, 5), 2.0)])
        self.assertEqual(len(self.store._daily_files()), 1)
        self.assertEqual(self.stored("SELECT date, cost, row_count FROM ROWS"), [(dt.date(2026, 10, 1), 3.0, 2)])
        self.assertEqual(self.store.export_time_range(), (utc(2026, 10, 1, 5), utc(2026, 10, 2, 5)))

    def test_months_older_than_daily_months_are_rolled_up(self):
        self.add([
            row(utc(2026, 6, 3), utc(2026, 6, 4), 1.0),
            row(utc(2026, 6, 20), utc(2026, 6, 21), 2.0),
            row(utc(2026, 7, 5), utc(2026, 7, 6), 4.0),
            row(utc(2026, 8, 5), utc(2026, 8, 6), 8.0),
            row(utc(2026, 9, 5), utc(2026, 9, 6), 16.0),
        ])
        self.assertEqual(sorted(p.name for p in (self.root / "daily").iterdir()), ["2026-07", "2026-08", "2026-09"])
        self.assertEqual(self.query("SELECT grain, date, cost, row_count FROM ROWS", self.store._monthly_files()),
                         [("month", dt.date(2026, 6, 1), 3.0, 2)])
        # A late row for a rolled-up month is added to its month file.
        self.add([row(utc(2026, 6, 30), utc(2026, 9, 7), 32.0)])
        self.assertEqual(self.query("SELECT date, cost, row_count FROM ROWS", self.store._monthly_files()),
                         [(dt.date(2026, 6, 1), 35.0, 3)])
        self.assertFalse((self.root / "daily" / "2026-06").exists())
        self.assertEqual(self.stored("SELECT SUM(cost) FROM ROWS"), [(63.0,)])

    def test_hourly_file_keeps_the_last_24_hours_of_rows(self):
        self.add([row(utc(2026, 10, 1, 1), utc(2026, 10, 1, 2), 1.0)])
        self.add([row(utc(2026, 10, 2, 1), utc(2026, 10, 2, 1), 2.0), row(utc(2026, 10, 2, 2), utc(2026, 10, 2, 3), 4.0)])
        hourly = self.store._hourly_file()
        self.assertEqual(self.query("SELECT cost FROM ROWS ORDER BY cost", [hourly]), [(2.0,), (4.0,)])

    def test_nothing_staged_changes_nothing(self):
        self.assertEqual(self.store.add(), 0)
        self.assertIsNone(self.store.export_time_range())


class FlatSelectListTest(unittest.TestCase):
    KV = [SchemaField("key", "STRING"), SchemaField("value", "STRING")]

    def schema(self, resource=True):
        fields = [
            SchemaField("service", "RECORD", fields=[SchemaField("id", "STRING"), SchemaField("description", "STRING")]),
            SchemaField("labels", "RECORD", mode="REPEATED", fields=self.KV),
            SchemaField("credits", "RECORD", mode="REPEATED", fields=[SchemaField("amount", "FLOAT")]),
            SchemaField("cost", "FLOAT"),
        ]
        if resource:
            fields.append(SchemaField("resource", "RECORD", fields=[SchemaField("name", "STRING"),
                                                                    SchemaField("global_name", "STRING")]))
        return fields

    def test_nested_fields_labels_and_credits_become_columns(self):
        sql = gcp_storage.flat_select_list(self.schema())
        self.assertIn("service.description AS service__description", sql)
        self.assertIn("resource.name AS resource__name", sql)
        self.assertIn("l.key = 'k8s-workload-name') AS k8s_workload", sql)
        self.assertIn("IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0) AS credits_total", sql)

    def test_missing_fields_are_null(self):
        sql = gcp_storage.flat_select_list(self.schema(resource=False))
        self.assertIn("CAST(NULL AS STRING) AS resource__name", sql)
        self.assertIn("CAST(NULL AS TIMESTAMP) AS usage_start_time", sql)
        self.assertIn("CAST(NULL AS FLOAT64) AS usage__amount", sql)


if __name__ == "__main__":
    unittest.main()
