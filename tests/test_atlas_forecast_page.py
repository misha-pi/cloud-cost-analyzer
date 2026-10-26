"""
Tests for scripts/generate_atlas_forecast_page.py (no Rill needed).

Run: uv run python -m unittest tests/test_atlas_forecast_page.py -v
"""
import datetime as dt
import pathlib
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import generate_atlas_forecast_page as page  # noqa: E402


def write_line_items(directory, rows, name="items.parquet"):
    days, projects, costs = zip(*rows)
    table = pa.table({
        "date": pa.array(days, pa.date32()),
        "project_name": pa.array(projects, pa.string()),
        "cost": pa.array(costs, pa.float64()),
    })
    pq.write_table(table, pathlib.Path(directory) / name)


class CountProjectsTest(unittest.TestCase):
    def test_counts_projects_in_the_12_complete_months(self):
        with tempfile.TemporaryDirectory() as d:
            write_line_items(d, [
                (dt.date(2025, 9, 30), "Too old", 1.0),       # before the 12 months
                (dt.date(2025, 10, 1), "First month", 1.0),
                (dt.date(2026, 9, 30), "Last month", 1.0),
                (dt.date(2026, 9, 15), None, 1.0),            # org-level line: one "(no project)" line
                (dt.date(2026, 9, 16), None, 2.0),
                (dt.date(2026, 9, 1), "No cost", None),       # not a cost row, as in atlas_costs
                (dt.date(2026, 10, 19), "Open month only", 1.0),  # latest month is still being billed
            ])
            self.assertEqual(page.count_projects(pathlib.Path(d)), 3)

    def test_reads_every_file(self):
        with tempfile.TemporaryDirectory() as d:
            write_line_items(d, [(dt.date(2026, 9, 1), "A", 1.0)], "a.parquet")
            write_line_items(d, [(dt.date(2026, 10, 1), "B", 1.0)], "b.parquet")
            self.assertEqual(page.count_projects(pathlib.Path(d)), 1)

    def test_no_data(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(page.count_projects(pathlib.Path(d)), 0)


class RenderTest(unittest.TestCase):
    def test_one_chart_per_project_two_per_row(self):
        yaml = page.render(5)
        for rank in range(1, 6):
            self.assertIn(f"rank IN ('{rank}')", yaml)
        self.assertNotIn("rank IN ('6')", yaml)
        self.assertEqual(yaml.count("Project #"), 5)
        # 3 rows of project charts: 2 + 2 + 1
        self.assertEqual(yaml.count("    height: 300px"), 3)

    def test_total_chart_and_page_parts_are_always_there(self):
        yaml = page.render(0)
        self.assertIn("rank IN ('0')", yaml)
        self.assertNotIn("Project #", yaml)
        for part in ("kpi_grid:", "pivot:", "metrics_sql", "theme: theme"):
            self.assertIn(part, yaml)

    def test_table_grows_with_projects(self):
        self.assertIn("height: 1344px", page.render(51))


if __name__ == "__main__":
    unittest.main()
