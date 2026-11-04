"""
Tests for the AWS Cost Explorer loader (pipelines/aws_cost_explorer_pipeline.py). No AWS needed.

Run: uv run python -m unittest tests/test_aws_cost_explorer.py -v
"""
import datetime as dt
import pathlib
import sys
import tempfile
import unittest

import pyarrow.parquet as pq

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipelines"))

import aws_cost_explorer_pipeline as aws  # noqa: E402


def money(amount):
    return {"Amount": str(amount), "Unit": "USD"}


class FakeClient:
    """Answers GetCostAndUsage with one group per service and second key for every day asked for."""

    def __init__(self, estimated_from=None, fail_on=None, page_size=None, tags=None):
        self.calls = []
        self.estimated_from = estimated_from  # days on or after this are estimated
        self.fail_on = fail_on  # (start, dimension) that raises
        self.page_size = page_size
        self.tags = tags or {}

    def get_cost_and_usage(self, TimePeriod, Granularity, Metrics, GroupBy, NextPageToken=None):
        start = dt.date.fromisoformat(TimePeriod["Start"])
        end = dt.date.fromisoformat(TimePeriod["End"])
        second = GroupBy[1]
        self.calls.append((start, end, second["Key"], NextPageToken))
        if self.fail_on == (start, second["Key"]):
            raise RuntimeError("AccessDenied")
        days = [start + dt.timedelta(n) for n in range((end - start).days)]
        if self.page_size:
            page = int(NextPageToken or 0)
            days, more = days[page:page + self.page_size], page + self.page_size < len(days)
        else:
            more = False
        results = []
        for day in days:
            if second["Type"] == "TAG":
                keys = [("Amazon S3", f"{second['Key']}${v}") for v in self.tags.get(second["Key"], [""])]
            else:
                keys = [("Amazon S3", "111"), ("Vercel", "111")] if second["Key"] == "LINKED_ACCOUNT" else \
                       [("Amazon S3", "us-east-1"), ("Vercel", "")] if second["Key"] == "REGION" else \
                       [("Amazon S3", "TimedStorage-ByteHrs"), ("Vercel", "Subscription")]
            groups = []
            for service, key in keys:
                metrics = {"UnblendedCost": money(10 if service == "Vercel" else 2),
                           "NetUnblendedCost": money(10 if service == "Vercel" else 1.5)}
                if "UsageQuantity" in Metrics:
                    metrics["UsageQuantity"] = {"Amount": "3", "Unit": "GB-Mo"}
                groups.append({"Keys": [service, key], "Metrics": metrics})
            estimated = self.estimated_from is not None and day >= self.estimated_from
            results.append({"TimePeriod": {"Start": day.isoformat()}, "Estimated": estimated, "Groups": groups})
        response = {"ResultsByTime": results,
                    "DimensionValueAttributes": [{"Value": "111", "Attributes": {"description": "SocialChain"}}]}
        if more:
            response["NextPageToken"] = str(int(NextPageToken or 0) + self.page_size)
        return response


class LoadTest(unittest.TestCase):
    def setUp(self):
        self.root = pathlib.Path(tempfile.mkdtemp()) / "aws_costs"

    def read(self, table, month):
        return pq.read_table(aws.month_file(self.root, table, month)).to_pylist()

    def test_first_load_writes_every_month_of_history(self):
        client = FakeClient()
        months = aws.load(self.root, aws.CostExplorer(client), dt.date(2026, 11, 4))
        self.assertEqual(len(months), aws.HISTORY_MONTHS)
        self.assertEqual((months[0], months[-1]), (dt.date(2025, 11, 1), dt.date(2026, 11, 1)))
        for table in aws.SCHEMAS:
            self.assertTrue(aws.month_file(self.root, table, dt.date(2025, 11, 1)).exists(), table)
        # The current month stops at yesterday.
        self.assertEqual(max(r["date"] for r in self.read("by_account", dt.date(2026, 11, 1))), dt.date(2026, 11, 3))

    def test_next_day_reloads_current_month_and_estimated_previous_month_only(self):
        aws.load(self.root, aws.CostExplorer(FakeClient(estimated_from=dt.date(2026, 10, 30))), dt.date(2026, 11, 4))
        client = FakeClient()
        months = aws.load(self.root, aws.CostExplorer(client), dt.date(2026, 11, 5))
        self.assertEqual(months, [dt.date(2026, 10, 1), dt.date(2026, 11, 1)])
        self.assertEqual(len(client.calls), 2 * 3)  # 3 tables (no tag keys) for 2 months
        # Once October is final, only November is downloaded.
        client = FakeClient()
        self.assertEqual(aws.load(self.root, aws.CostExplorer(client), dt.date(2026, 11, 6)), [dt.date(2026, 11, 1)])
        self.assertEqual(len(client.calls), 3)

    def test_rows_split_vercel_and_keep_estimated_flag(self):
        aws.load(self.root, aws.CostExplorer(FakeClient(estimated_from=dt.date(2026, 11, 2))), dt.date(2026, 11, 3))
        rows = self.read("by_account", dt.date(2026, 11, 1))
        self.assertEqual({(r["service"], r["provider"]) for r in rows},
                         {("Amazon S3", "AWS"), ("Vercel", "Vercel (via AWS)")})
        self.assertEqual({r["account_name"] for r in rows}, {"SocialChain"})
        self.assertEqual({(r["date"], r["estimated"]) for r in rows},
                         {(dt.date(2026, 11, 1), False), (dt.date(2026, 11, 2), True)})
        self.assertEqual(sum(r["net_cost"] for r in rows), 2 * (1.5 + 10))
        regions = {r["region"] for r in self.read("by_region", dt.date(2026, 11, 1))}
        self.assertEqual(regions, {"us-east-1", "global"})
        usage = self.read("by_usage_type", dt.date(2026, 11, 1))[0]
        self.assertEqual((usage["usage_quantity"], usage["usage_unit"]), (3.0, "GB-Mo"))

    def test_marketplace_products_match_by_name_prefix(self):
        explorer = aws.CostExplorer(FakeClient())
        self.assertEqual(explorer.provider("Vercel"), "Vercel (via AWS)")
        self.assertEqual(explorer.provider("Vercel Frontend Cloud"), "Vercel (via AWS)")
        self.assertEqual(explorer.provider("VercelX"), "AWS")
        self.assertEqual(explorer.provider("Amazon Simple Storage Service"), "AWS")

    def test_tag_table_one_set_per_key_and_untagged_cost(self):
        client = FakeClient(tags={"app": ["kyc", ""]})
        aws.load(self.root, aws.CostExplorer(client), dt.date(2026, 11, 2), tag_keys=["app"])
        rows = self.read("by_tag", dt.date(2026, 11, 1))
        self.assertEqual(sorted(r["tag_value"] for r in rows), ["(no tag)", "kyc"])
        self.assertEqual({r["tag_key"] for r in rows}, {"app"})

    def test_no_tag_keys_writes_an_empty_tag_table(self):
        aws.load(self.root, aws.CostExplorer(FakeClient()), dt.date(2026, 11, 2))
        self.assertEqual(self.read("by_tag", dt.date(2026, 11, 1)), [])

    def test_pages_are_followed(self):
        client = FakeClient(page_size=2)
        aws.load(self.root, aws.CostExplorer(client), dt.date(2026, 11, 6))
        self.assertEqual(len({r["date"] for r in self.read("by_region", dt.date(2026, 11, 1))}), 5)

    def test_first_of_month_writes_empty_current_month_without_a_request(self):
        aws.load(self.root, aws.CostExplorer(FakeClient(estimated_from=dt.date(2026, 10, 30))), dt.date(2026, 10, 31))
        client = FakeClient()
        aws.load(self.root, aws.CostExplorer(client), dt.date(2026, 11, 1))
        self.assertTrue(all(start.month == 10 for start, *_ in client.calls))
        self.assertEqual(self.read("by_account", dt.date(2026, 11, 1)), [])

    def test_failed_request_leaves_files_untouched(self):
        aws.load(self.root, aws.CostExplorer(FakeClient(estimated_from=dt.date(2026, 11, 1))), dt.date(2026, 11, 3))
        before = self.read("by_account", dt.date(2026, 11, 1))
        client = FakeClient(fail_on=(dt.date(2026, 11, 1), "REGION"))
        with self.assertRaises(RuntimeError):
            aws.load(self.root, aws.CostExplorer(client), dt.date(2026, 11, 4))
        self.assertEqual(self.read("by_account", dt.date(2026, 11, 1)), before)
        self.assertEqual(list((self.root / "by_account").glob(".*.tmp")), [])


    def test_empty_months_only_fill_gaps(self):
        aws.load(self.root, aws.CostExplorer(FakeClient()), dt.date(2026, 11, 3))
        before = self.read("by_account", dt.date(2026, 11, 1))
        (aws.month_file(self.root, "by_region", dt.date(2026, 5, 1))).unlink()
        aws.write_empty_months(self.root, dt.date(2026, 11, 3))
        self.assertEqual(self.read("by_account", dt.date(2026, 11, 1)), before)
        self.assertEqual(self.read("by_region", dt.date(2026, 5, 1)), [])


if __name__ == "__main__":
    unittest.main()
