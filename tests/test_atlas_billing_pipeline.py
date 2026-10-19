"""
Tests for the MongoDB Atlas billing pipeline.

The tests read the sample data in fixtures/atlas_billing/, made by scripts/generate_atlas_fixtures.py
(made-up values in the production schema). A test checks the files still match the generator.

Run: make test-atlas
  or uv run python -m unittest tests/test_atlas_billing_pipeline.py -v
"""
import datetime as dt
import json
import pathlib
import sys
import tempfile
import unittest
from collections import defaultdict
from decimal import Decimal

from bson import Decimal128, ObjectId

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipelines"))
sys.path.insert(0, str(ROOT / "scripts"))

import generate_atlas_fixtures  # noqa: E402
import mongodb_atlas_billing_pipeline as atlas  # noqa: E402

FIXTURES = ROOT / "fixtures" / "atlas_billing"

# Field paths and canonical Extended JSON types seen in production samples of atlas_billing.
# The generated documents must use exactly these, so the pipeline is tested against the real schema.
LINE_ITEM_SCHEMA = {
    "_id": {"$oid"},
    "category": {"str"},
    "cluster": {"str"},
    "cost": {"$numberDecimal"},
    "date": {"$date"},
    "instance": {"str"},
    "invoiceId": {"str"},
    "org": {"dict"},
    "org.id": {"str"},
    "org.name": {"str"},
    "project": {"dict"},
    "project.id": {"str"},
    "project.name": {"str"},
    "provider": {"str"},
    "quantity": {"$numberDouble", "$numberInt"},
    "sku": {"str"},
    "tags": {"dict"},
    "unit": {"str"},
    "unitPriceDollars": {"$numberDecimal"},
}
INVOICE_SCHEMA = {
    "_id": {"$oid"},
    "amountBilledCents": {"$numberInt"},
    "amountPaidCents": {"$numberInt"},
    "created": {"str"},
    "creditsCents": {"$numberInt"},
    "endDate": {"str"},
    "id": {"str"},
    "lineItems": {"list"},
    "lineItems[].cloudProvider": {"str"},
    "lineItems[].clusterName": {"str"},
    "lineItems[].created": {"str"},
    "lineItems[].endDate": {"str"},
    "lineItems[].groupId": {"str"},
    "lineItems[].groupName": {"str"},
    "lineItems[].quantity": {"$numberDouble", "$numberInt"},
    "lineItems[].sku": {"str"},
    "lineItems[].startDate": {"str"},
    "lineItems[].totalPriceCents": {"$numberInt"},
    "lineItems[].unit": {"str"},
    "lineItems[].unitPriceDollars": {"$numberDouble", "$numberInt"},
    "links": {"list"},
    "links[].href": {"str"},
    "links[].rel": {"str"},
    "orgId": {"str"},
    "payments": {"list"},
    "payments[].amountBilledCents": {"$numberInt"},
    "payments[].amountPaidCents": {"$numberInt"},
    "payments[].created": {"str"},
    "payments[].currency": {"str"},
    "payments[].id": {"str"},
    "payments[].salesTaxCents": {"$numberInt"},
    "payments[].statusName": {"str"},
    "payments[].subtotalCents": {"$numberInt"},
    "payments[].unitPrice": {"str"},
    "payments[].updated": {"NoneType"},
    "refunds": {"list"},
    "salesTaxCents": {"$numberInt"},
    "startDate": {"str"},
    "startingBalanceCents": {"$numberInt"},
    "statusName": {"str"},
    "subtotalCents": {"$numberInt"},
    "updated": {"str"},
    "linkedInvoiceIds": {"list"},
    "segment": {"$numberInt"},
}


def _raw(collection: str) -> list[dict]:
    """Fixture documents as plain JSON, keeping the Extended JSON type wrappers."""
    with open(FIXTURES / f"{collection}.json") as f:
        return [json.loads(line) for line in f if line.strip()]


def _bson(collection: str) -> list[dict]:
    return list(atlas._read_fixture(FIXTURES, collection))


def _type(value) -> str:
    if isinstance(value, dict) and len(value) == 1 and next(iter(value)).startswith("$"):
        return next(iter(value))
    return type(value).__name__


def _paths(doc: dict, prefix: str = "") -> set[tuple[str, str]]:
    out = set()
    for key, value in doc.items():
        path = prefix + key
        out.add((path, _type(value)))
        if isinstance(value, dict) and not _type(value).startswith("$"):
            out |= _paths(value, path + ".")
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    out |= _paths(item, path + "[].")
    return out


class FixtureSchemaTest(unittest.TestCase):
    def assert_schema(self, docs: list[dict], schema: dict[str, set[str]]):
        seen = defaultdict(set)
        for doc in docs:
            for path, kind in _paths(doc):
                seen[path].add(kind)
        self.assertEqual(set(seen), set(schema), "field paths differ from the production schema")
        for path, kinds in seen.items():
            self.assertLessEqual(kinds, schema[path], f"{path} has types {kinds}")

    def test_line_items_match_production_schema(self):
        self.assert_schema(_raw("lineItems"), LINE_ITEM_SCHEMA)

    def test_invoices_match_production_schema(self):
        self.assert_schema(_raw("invoices"), INVOICE_SCHEMA)

    def test_fixtures_cover_the_odd_cases(self):
        docs = _raw("lineItems")
        self.assertTrue(any(d["project"] == {} for d in docs), "a Support line without project")
        self.assertTrue(any(d["cluster"] == atlas.NO_CLUSTER for d in docs), "an org-level line")
        self.assertTrue(any("tags" in d for d in docs), "a line with tags")
        self.assertTrue(any(d["segment"]["$numberInt"] != "0" for d in _raw("invoices")), "a split invoice")

    def test_closed_invoices_equal_their_line_items(self):
        cents = defaultdict(Decimal)
        for doc in _bson("lineItems"):
            cents[doc["invoiceId"]] += doc["cost"].to_decimal() * 100
        invoices = {}
        for doc in _bson("invoices"):
            invoices.setdefault(doc["id"], doc)
        self.assertTrue(invoices)
        for invoice_id, doc in invoices.items():
            if doc["statusName"] != "PENDING":
                self.assertEqual(cents[invoice_id], doc["subtotalCents"], invoice_id)

    def test_fixtures_match_the_generator(self):
        # Regenerate with the defaults; the committed files must be identical.
        # If this fails: python scripts/generate_atlas_fixtures.py --out fixtures/atlas_billing
        with tempfile.TemporaryDirectory() as tmp:
            generate_atlas_fixtures.generate(pathlib.Path(tmp))
            for name in ("invoices.json", "lineItems.json"):
                self.assertEqual((pathlib.Path(tmp) / name).read_bytes(), (FIXTURES / name).read_bytes(), name)

    def test_split_invoices_repeat_the_header(self):
        headers = defaultdict(set)
        for doc in _bson("invoices"):
            headers[doc["id"]].add((doc["subtotalCents"], doc["amountBilledCents"], doc["statusName"]))
        for invoice_id, values in headers.items():
            self.assertEqual(len(values), 1, invoice_id)


class LineItemRowTest(unittest.TestCase):
    def doc(self, **overrides):
        doc = {
            "_id": ObjectId("0123456789abcdef01234567"),
            "category": "Instances",
            "cluster": "orders",
            "cost": Decimal128("79.2000000000000000"),
            "date": dt.datetime(2026, 9, 3, tzinfo=dt.timezone.utc),
            "instance": "M40",
            "invoiceId": "87a2f545bd5a48b53acfb271",
            "org": {"id": "22ba8f83a9ae698c4b712c19", "name": "Example Org"},
            "project": {"id": "b596f4d9863b87440d2abac3", "name": "Orders - Production"},
            "provider": "GCP",
            "quantity": 72,
            "sku": "ATLAS_GCP_INSTANCE_M40",
            "unit": "server hours",
            "unitPriceDollars": Decimal128("1.1000000000000000"),
        }
        doc.update(overrides)
        return doc

    def test_cluster_line(self):
        row = atlas.line_item_row(self.doc())
        self.assertEqual(row["date"], dt.date(2026, 9, 3))
        self.assertEqual(row["project_name"], "Orders - Production")
        self.assertEqual(row["cluster"], "orders")
        self.assertTrue(row["is_cluster_cost"])
        self.assertEqual(row["cost"], Decimal("79.2"))
        self.assertEqual(row["quantity"], 72.0)
        self.assertIsNone(row["tags"])

    def test_support_line_without_project(self):
        row = atlas.line_item_row(self.doc(category="Support", cluster=atlas.NO_CLUSTER, project={},
                                           provider="n/a", instance="non-instance", tags={}))
        self.assertIsNone(row["project_id"])
        self.assertIsNone(row["project_name"])
        self.assertFalse(row["is_cluster_cost"])
        self.assertIsNone(row["tags"])

    def test_tags_are_kept_as_json(self):
        row = atlas.line_item_row(self.doc(tags={"env": "prod", "app": "orders"}))
        self.assertEqual(json.loads(row["tags"]), {"app": "orders", "env": "prod"})


class InvoiceRowTest(unittest.TestCase):
    def test_header_in_dollars(self):
        row = atlas.invoice_row({
            "_id": ObjectId("abcdefabcdefabcdefabcdef"), "id": "fedcba9876543210fedcba98", "segment": 1,
            "orgId": "x", "statusName": "PAID", "startDate": "2026-07-01T00:00:00Z",
            "endDate": "2026-08-01T00:00:00Z", "created": "2026-07-01T02:00:25Z", "updated": "2026-08-01T02:00:23Z",
            "subtotalCents": 1234567, "creditsCents": 1234567, "salesTaxCents": 0, "startingBalanceCents": 0,
            "amountBilledCents": 1234567, "amountPaidCents": 0, "lineItemCount": 2033,
        })
        self.assertEqual(row["invoice_id"], "fedcba9876543210fedcba98")
        self.assertEqual(row["segment"], 1)
        self.assertEqual(row["start_date"], dt.date(2026, 7, 1))
        self.assertEqual(row["subtotal"], Decimal("12345.67"))
        self.assertEqual(row["amount_paid"], Decimal(0))
        self.assertEqual(row["embedded_line_item_count"], 2033)


class SourceFixtureModeTest(unittest.TestCase):
    def test_reads_fixtures_from_start_date(self):
        source = atlas.atlas_billing_source(initial_start_date="2026-10-01T00:00:00Z", fixtures_dir=str(FIXTURES))
        rows = list(source.with_resources("line_items"))
        expected = sum(1 for d in _bson("lineItems") if d["date"] >= dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc))
        self.assertEqual(len(rows), expected)
        self.assertTrue(all(r["date"] >= dt.date(2026, 10, 1) for r in rows))

    def test_invoices_without_embedded_line_items(self):
        source = atlas.atlas_billing_source(fixtures_dir=str(FIXTURES))
        rows = list(source.with_resources("invoices"))
        self.assertEqual(len(rows), len(_raw("invoices")))
        self.assertTrue(all(r["embedded_line_item_count"] > 0 for r in rows))
        self.assertNotIn("lineItems", rows[0])


if __name__ == "__main__":
    unittest.main()
