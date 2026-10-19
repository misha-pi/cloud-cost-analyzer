"""
Generate made-up MongoDB Atlas billing documents for the tests and the local demo.

Nothing generated here is committed: the tests write to a temporary folder, and `make demo` /
`make atlas-demo-data` load the documents into viz_rill/data (gitignored) through the pipeline.

The documents have the same collections, field names, key order and BSON types as our
`atlas_billing` database (see docs/atlas_billing_schema.md), but every value is made up:
org, project and cluster names, IDs, quantities, prices and costs. Nothing is copied from production.

The numbers are internally consistent, like the real data:
- each line item's `cost` is in dollars, rounded to the cent;
- an invoice's `subtotalCents` is the sum of its line items' cost (except the PENDING invoice,
  whose subtotal is not filled in until the month closes);
- a PAID invoice is covered by credits: `creditsCents == amountBilledCents` and
  `amountPaidCents` is 0;
- invoices embed their line items, at most --cap per document. A bigger invoice is split into
  several documents with the same `id` and header and `segment` 0, 1, ... (production caps at 10,000);
- org-level lines (Support, Data Lake) have cluster "--n/a--"; Support lines have `project: {}`;
- about 2% of line items carry an empty `tags: {}`, as in production.

Writes one canonical Extended JSON document per line (the format of
`mongoexport --jsonFormat=canonical`), so the files can also be loaded with `mongoimport`.

Usage:
  python scripts/generate_atlas_fixtures.py --out /tmp/atlas_fixtures                  # Sep - Oct 2026
  python scripts/generate_atlas_fixtures.py --start 2025-01-01 --out /tmp/atlas_demo   # longer demo
"""
import argparse
import datetime as dt
import pathlib
import random
from decimal import ROUND_HALF_UP, Decimal

from bson import Decimal128, ObjectId, json_util

NO_CLUSTER = "--n/a--"  # Atlas's cluster value for org-level lines

# project -> clusters: (name, size before UPSIZE_DAY, size from UPSIZE_DAY, first day, last day)
UPSIZE_DAY = dt.date(2026, 4, 1)
PROJECTS = {
    "Orders - Production": [
        ("orders", "M40", "M50", None, None),
        ("orders-archive", "M10", "M10", None, dt.date(2026, 5, 31)),  # deleted end of May 2026
    ],
    "Orders - Staging": [("orders", "M10", "M10", None, None)],
    "Orders - Development": [("orders", "M10", "M10", None, None)],
    "Payments - Production": [("payments", "M30", "M40", None, None)],
    "Payments - Staging": [("payments", "M10", "M10", None, None)],
    "Search - Production": [("search", "M60_LOW_CPU", "M60_LOW_CPU", dt.date(2025, 6, 1), None)],
    "Reports - Production": [("reports", "M20", "M30", None, None)],
    "Chat - Production": [("chat", "M30", "M30", None, None)],
    "Chat - Staging": [("chat", "M10", "M10", None, None)],
}
HOURLY_PRICE = {"M10": 0.09, "M20": 0.21, "M30": 0.57, "M40": 1.10, "M50": 2.10, "M60_LOW_CPU": 2.45}


DEFAULT_START = dt.date(2026, 9, 1)
DEFAULT_END = dt.date(2026, 10, 15)
DEFAULT_CAP = 500  # small, so a test invoice is split into segments like a big production one
DEFAULT_SEED = 20261016


def parse_args():
    p = argparse.ArgumentParser(description="Generate made-up Atlas billing documents.")
    p.add_argument("--out", type=pathlib.Path, required=True, help="folder for invoices.json and lineItems.json")
    p.add_argument("--start", type=dt.date.fromisoformat, default=DEFAULT_START)
    p.add_argument("--end", type=dt.date.fromisoformat, default=DEFAULT_END,
                   help="last usage day; its month's invoice is PENDING")
    p.add_argument("--cap", type=int, default=DEFAULT_CAP, help="embedded line items per invoice document")
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    return p.parse_args()


class Generator:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)

    def object_id(self) -> ObjectId:
        return ObjectId(bytes(self.rng.getrandbits(8) for _ in range(12)))

    def hex_id(self) -> str:
        return str(self.object_id())

    def dec(self, value) -> Decimal128:
        """Dollar amounts are Decimal128; production values carry 13 to 16 decimal places."""
        places = self.rng.choice([13, 14, 15, 16, 16, 16])
        return Decimal128(Decimal(str(value)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def cents(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def quantity(value: float):
    """Whole quantities come back as Int32, the rest as Double."""
    return int(value) if float(value).is_integer() else float(value)


def iso(day: dt.date, time: str = "00:00:00") -> str:
    return f"{day.isoformat()}T{time}Z"


def months_between(a: dt.date, b: dt.date) -> int:
    return (b.year - a.year) * 12 + b.month - a.month


def daily_lines(g: Generator, project: str, size: str, day: dt.date, growth: float) -> list[tuple]:
    """(category, sku, unit, instance, provider, quantity, unit price) billed by one cluster on one day."""
    rng = g.rng
    production = project.endswith("Production")
    weekday = 1.0 if day.weekday() < 5 else 0.7
    lines = [("Instances", f"ATLAS_GCP_INSTANCE_{size}", "server hours", size, "GCP",
              72 if production else 24, HOURLY_PRICE[size])]
    lines.append(("Data Transfer", "ATLAS_GCP_DATA_TRANSFER_INTER_ZONE", "GB", "non-instance", "GCP",
                  round(rng.uniform(1, 60) * growth * weekday, 6), 0.01))
    if production:
        lines.append(("Storage", "ATLAS_GCP_STORAGE_SSD", "GB hours", "non-instance", "GCP",
                      int(rng.uniform(80, 400) * growth) * 24, 0.00024))
        lines.append(("Backup", "ATLAS_NDS_GCP_PIT_RESTORE_STORAGE", "GB days", "non-instance", "GCP",
                      round(rng.uniform(5, 300) * growth, 6), 0.0021))
        lines.append(("Data Transfer", "ATLAS_GCP_DATA_TRANSFER_INTERNET", "GB", "non-instance", "GCP",
                      round(rng.uniform(1, 120) * growth * weekday, 6), 0.12))
    return lines


def generate(out: pathlib.Path, start: dt.date = DEFAULT_START, end: dt.date = DEFAULT_END,
             cap: int = DEFAULT_CAP, seed: int = DEFAULT_SEED) -> None:
    """Write invoices.json and lineItems.json for usage days start..end into `out`."""
    g = Generator(seed)
    org = {"id": g.hex_id(), "name": "Example Org"}
    project_ids = {name: g.hex_id() for name in PROJECTS}

    # (day, project or None, cluster, category, sku, unit, instance, provider, quantity, unit price)
    raw = []
    day = start
    while day <= end:
        growth = 1 + 0.025 * months_between(start, day)
        for project, clusters in PROJECTS.items():
            for cluster, size, upsized, first, last in clusters:
                if (first and day < first) or (last and day > last):
                    continue
                current = upsized if day >= UPSIZE_DAY else size
                for line in daily_lines(g, project, current, day, growth):
                    raw.append((day, project, cluster, *line))
        # Org-level Support plan, billed per day, with no project.
        raw.append((day, None, NO_CLUSTER, "Support", "ATLAS_SUPPORT", "days", "non-instance", "n/a", 1, 75.99))
        if day.day % 3 == 0:
            raw.append((day, "Reports - Production", NO_CLUSTER, "Data Lake", "ATLAS_DATA_LAKE_AWS_DATA_SCANNED",
                        "TB", "non-instance", "AWS", round(g.rng.uniform(0.01, 0.4), 6), 5.0))
        day += dt.timedelta(days=1)

    months = sorted({(d.year, d.month) for d, *_ in raw})
    invoice_ids = {m: g.hex_id() for m in months}

    line_items, embedded = [], {m: [] for m in months}
    for day, project, cluster, category, sku, unit, instance, provider, qty, price in raw:
        month = (day.year, day.month)
        cost = cents(qty * price)
        doc = {
            "_id": g.object_id(),
            "category": category,
            "cluster": cluster,
            "cost": g.dec(cost),
            "date": dt.datetime.combine(day, dt.time(), tzinfo=dt.timezone.utc),
            "instance": instance,
            "invoiceId": invoice_ids[month],
            "org": org,
            "project": {"id": project_ids[project], "name": project} if project else {},
            "provider": provider,
            "quantity": quantity(qty),
            "sku": sku,
        }
        if g.rng.random() < 0.02:
            doc["tags"] = {}  # production has an empty tags object on ~2% of lines, between sku and unit
        doc |= {"unit": unit, "unitPriceDollars": g.dec(price)}
        line_items.append(doc)

        item = {"cloudProvider": provider if provider != "n/a" else "NONE"}
        if cluster != NO_CLUSTER:
            item["clusterName"] = cluster
        item |= {"created": iso(day + dt.timedelta(days=1), "02:12:52"), "endDate": iso(day + dt.timedelta(days=1))}
        if project:
            item |= {"groupId": project_ids[project], "groupName": project}
        item |= {
            "quantity": quantity(qty),
            "sku": sku,
            "startDate": iso(day),
            "totalPriceCents": int(cost * 100),
            "unit": unit,
            "unitPriceDollars": quantity(price),
        }
        embedded[month].append(item)

    invoices = []
    for month in months:
        year, mon = month
        month_start = dt.date(year, mon, 1)
        month_end = dt.date(year + (mon == 12), mon % 12 + 1, 1)
        pending = month == months[-1]
        subtotal = 12 if pending else sum(i["totalPriceCents"] for i in embedded[month])
        billed = subtotal  # no sales tax
        inv_id = invoice_ids[month]
        header = {
            "amountBilledCents": billed,
            "amountPaidCents": 0,  # PAID invoices are covered by credits
            "created": iso(month_start, "02:00:25"),
            "creditsCents": g.rng.randint(1_000, 50_000) if pending else billed,
            "endDate": iso(month_end),
            "id": inv_id,
        }
        tail = {
            "links": [
                {"href": f"https://cloud.mongodb.com/api/atlas/v1.0/orgs/{org['id']}",
                 "rel": "https://cloud.mongodb.com/org"},
                {"href": f"https://cloud.mongodb.com/api/atlas/v1.0/orgs/{org['id']}/invoices/{inv_id}",
                 "rel": "self"},
            ],
            "orgId": org["id"],
            "payments": [] if pending else [{
                "amountBilledCents": billed,
                "amountPaidCents": 0,
                "created": iso(month_end, "02:00:23"),
                "currency": "USD",
                "id": g.hex_id(),
                "salesTaxCents": 0,
                "statusName": "PAID",
                "subtotalCents": subtotal,
                "unitPrice": "1.0",
                "updated": None,
            }],
            "refunds": [],
            "salesTaxCents": 0,
            "startDate": iso(month_start),
            "startingBalanceCents": 0,
            "statusName": "PENDING" if pending else "PAID",
            "subtotalCents": subtotal,
            "updated": iso(end if pending else month_end, "02:00:23"),
            "linkedInvoiceIds": [],
        }
        items = embedded[month]
        for segment, first in enumerate(range(0, len(items), cap)):
            invoices.append({"_id": g.object_id(), **header, "lineItems": items[first:first + cap],
                             **tail, "segment": segment})

    out.mkdir(parents=True, exist_ok=True)
    for name, docs in [("invoices", invoices), ("lineItems", line_items)]:
        path = out / f"{name}.json"
        with open(path, "w") as f:
            for doc in docs:
                f.write(json_util.dumps(doc, json_options=json_util.CANONICAL_JSON_OPTIONS, separators=(",", ":")) + "\n")
        print(f"Wrote {len(docs):,} documents to {path}")


if __name__ == "__main__":
    generate(**vars(parse_args()))
