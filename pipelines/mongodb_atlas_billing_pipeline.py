# flake8: noqa
"""
Load MongoDB Atlas billing from the `atlas_billing` database into the cost dashboards.

Our Atlas invoices are synced into a MongoDB database, `atlas_billing`:
- lineItems: one document per day, project, cluster and SKU, with `cost` in dollars. This is what
  the dashboards chart. Its costs add up exactly to each closed invoice's subtotal.
  Org-level lines (Support, Data Lake, ...) have cluster "--n/a--", and Support lines have no project
  (`project: {}`). `tags` is an empty object on a few lines; it is kept as JSON for when clusters get tags.
- invoices: one document per invoice (several when an invoice has more line items than one document
  holds: same `id` and header, `segment` 0, 1, ...). Only the header is loaded, for monthly totals.
The third collection, orgInfo, is not loaded: it holds the single org's id and name, which every
line item already carries. See docs/atlas_billing_schema.md for the schema.

Every run reloads everything from `initial_start_date` (write_disposition="replace"): the open
month's line items keep changing until the invoice closes, and the collections are small.

Connection, in this order:
- ATLAS_BILLING_FIXTURES_DIR: read invoices.json and lineItems.json (canonical Extended JSON, as written
  by scripts/generate_atlas_fixtures.py or mongoexport) from that folder instead of MongoDB, with no
  credentials. scripts/atlas_demo_data.sh uses this for the demo.
- `sources.atlas_billing.connection_string` in .dlt/secrets.toml.
- MONGO_BILLING_HOST, MONGO_BILLING_USER and MONGO_BILLING_PASSWORD environment variables
  (as in the server's mongo.env), with MONGO_BILLING_DATABASE overriding the database.
"""
import datetime as dt
import json
import os
import pathlib
from decimal import Decimal
from urllib.parse import quote_plus

import dlt
from bson import Decimal128, json_util

# Atlas's cluster value for org-level lines such as Support, Charts and Data Lake.
NO_CLUSTER = "--n/a--"

# Fixture dates load as UTC-aware datetimes, like pymongo's MongoClient(tz_aware=True).
_FIXTURE_JSON_OPTIONS = json_util.CANONICAL_JSON_OPTIONS.with_options(tz_aware=True)

# Print a line this often while reading line items. The first row is always reported.
_PROGRESS_EVERY_ROWS = 50_000


def _to_decimal(value) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal128):
        return value.to_decimal()
    return Decimal(str(value))


def _to_date(value) -> dt.date | None:
    """Line item dates are midnight UTC datetimes; invoice dates are ISO strings."""
    if value is None:
        return None
    if isinstance(value, str):
        value = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value.date()


def _parse_timestamp(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def line_item_row(doc: dict) -> dict:
    """Flatten one `lineItems` document into a dashboard row."""
    org = doc.get("org") or {}
    project = doc.get("project") or {}
    cluster = doc.get("cluster")
    return {
        "id": str(doc["_id"]),
        "date": _to_date(doc.get("date")),
        "invoice_id": doc.get("invoiceId"),
        "org_id": org.get("id"),
        "org_name": org.get("name"),
        "project_id": project.get("id"),
        "project_name": project.get("name"),
        "cluster": cluster,
        "is_cluster_cost": bool(cluster) and cluster != NO_CLUSTER,
        "category": doc.get("category"),
        "sku": doc.get("sku"),
        "instance": doc.get("instance"),
        "provider": doc.get("provider"),
        "unit": doc.get("unit"),
        "quantity": float(doc["quantity"]) if doc.get("quantity") is not None else None,
        "unit_price_dollars": _to_decimal(doc.get("unitPriceDollars")),
        "cost": _to_decimal(doc.get("cost")),
        # Atlas resource tags; always empty so far, so stored as JSON text rather than as columns.
        "tags": json.dumps(doc["tags"], sort_keys=True) if doc.get("tags") else None,
    }


def invoice_row(doc: dict) -> dict:
    """Flatten one `invoices` document's header (amounts in cents become dollars)."""

    def dollars(field: str) -> Decimal | None:
        value = doc.get(field)
        return None if value is None else Decimal(int(value)) / 100

    return {
        "id": str(doc["_id"]),
        "invoice_id": doc.get("id"),
        "segment": int(doc.get("segment") or 0),
        "org_id": doc.get("orgId"),
        "status": doc.get("statusName"),
        "start_date": _to_date(doc.get("startDate")),
        "end_date": _to_date(doc.get("endDate")),
        "created": _parse_timestamp(doc.get("created")),
        "updated": _parse_timestamp(doc.get("updated")),
        "subtotal": dollars("subtotalCents"),
        "credits": dollars("creditsCents"),
        "sales_tax": dollars("salesTaxCents"),
        "starting_balance": dollars("startingBalanceCents"),
        "amount_billed": dollars("amountBilledCents"),
        "amount_paid": dollars("amountPaidCents"),
        "embedded_line_item_count": int(doc.get("lineItemCount") or 0),
    }


def _read_fixture(fixtures_dir: pathlib.Path, collection: str):
    with open(fixtures_dir / f"{collection}.json") as f:
        for line in f:
            if line.strip():
                yield json_util.loads(line, json_options=_FIXTURE_JSON_OPTIONS)


def _connection_string() -> str:
    connection_string = dlt.secrets.get("sources.atlas_billing.connection_string")
    if connection_string:
        return connection_string
    host = os.getenv("MONGO_BILLING_HOST")
    user = os.getenv("MONGO_BILLING_USER")
    password = os.getenv("MONGO_BILLING_PASSWORD")
    if not (host and user and password):
        raise RuntimeError(
            "No Atlas billing connection: set sources.atlas_billing.connection_string in "
            ".dlt/secrets.toml, or MONGO_BILLING_HOST, MONGO_BILLING_USER and MONGO_BILLING_PASSWORD."
        )
    scheme = "mongodb" if ":" in host or host.startswith("localhost") else "mongodb+srv"
    return f"{scheme}://{quote_plus(user)}:{quote_plus(password)}@{host}/?readPreference=secondaryPreferred"


@dlt.source(name="atlas_billing")
def atlas_billing_source(
    database: str = "atlas_billing",
    initial_start_date: str | None = None,
    fixtures_dir: str | None = None,
):
    start = dt.datetime.fromisoformat((initial_start_date or "2000-01-01").replace("Z", "+00:00"))
    if start.tzinfo is None:
        start = start.replace(tzinfo=dt.timezone.utc)
    fixtures = pathlib.Path(fixtures_dir) if fixtures_dir else None

    def collection(name: str):
        from pymongo import MongoClient

        client = MongoClient(_connection_string(), tz_aware=True, appname="cloud-cost-analyzer")
        return client, client[database][name]

    @dlt.resource(name="line_items", write_disposition="replace", primary_key="id")
    def line_items():
        if fixtures:
            docs = (d for d in _read_fixture(fixtures, "lineItems") if d["date"] >= start)
        else:
            client, coll = collection("lineItems")
            docs = coll.find({"date": {"$gte": start}}, batch_size=5_000)
        row_count = 0
        for doc in docs:
            row_count += 1
            if row_count == 1 or row_count % _PROGRESS_EVERY_ROWS == 0:
                print(f"Read {row_count:,} Atlas line items; latest date={doc.get('date')}", flush=True)
            yield line_item_row(doc)
        print(f"Finished reading {row_count:,} Atlas line items", flush=True)
        if not fixtures:
            client.close()

    @dlt.resource(name="invoices", write_disposition="replace", primary_key="id")
    def invoices():
        # Invoice dates are ISO strings ("2026-09-01T00:00:00Z"), so compare them as strings.
        start_iso = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        if fixtures:
            docs = []
            for d in _read_fixture(fixtures, "invoices"):
                if d.get("endDate", "") > start_iso:
                    d["lineItemCount"] = len(d.pop("lineItems", []))
                    docs.append(d)
        else:
            client, coll = collection("invoices")
            # The embedded line items are large and duplicated in lineItems: only count them.
            docs = coll.aggregate([
                {"$match": {"endDate": {"$gt": start_iso}}},
                {"$addFields": {"lineItemCount": {"$size": {"$ifNull": ["$lineItems", []]}}}},
                {"$project": {"lineItems": 0}},
            ])
        for doc in docs:
            yield invoice_row(doc)
        if not fixtures:
            client.close()

    return line_items, invoices


def load_atlas_billing() -> None:
    destination = os.getenv("DLT_DESTINATION", "filesystem")

    try:
        pipeline_name = dlt.config["pipeline.pipeline_name"]
    except KeyError:
        pipeline_name = "cloud_cost_analytics"

    dataset_name = dlt.config.get("sources.atlas_billing.dataset_name") or "atlas_billing"
    database = os.getenv("MONGO_BILLING_DATABASE") or dlt.config.get("sources.atlas_billing.database") or "atlas_billing"
    initial_start_date = dlt.config.get("sources.atlas_billing.initial_start_date")
    fixtures_dir = os.getenv("ATLAS_BILLING_FIXTURES_DIR") or None

    pipeline = dlt.pipeline(
        pipeline_name=f"{pipeline_name}_atlas",
        destination=destination,
        dataset_name=dataset_name,
        progress="log",
    )
    source = atlas_billing_source(database=database, initial_start_date=initial_start_date, fixtures_dir=fixtures_dir)
    info = pipeline.run(source, loader_file_format="parquet")
    print(f"Pipeline {pipeline.pipeline_name} load step completed in {info}")


if __name__ == "__main__":
    load_atlas_billing()
