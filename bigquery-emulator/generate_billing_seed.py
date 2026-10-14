#!/usr/bin/env python3
"""
Generate synthetic data that mimics the GCP *standard* Cloud Billing export
table (gcp_billing_export_v1_<ACCOUNT>) and load it into either:

  * the local BigQuery emulator (goccy/bigquery-emulator), or
  * files for loading into a real BigQuery dev project (schema JSON + NDJSON).

All data is synthetic. Service / SKU IDs are placeholders in the real
"XXXX-XXXX-XXXX" format, not Google's actual IDs.

Built-in realism, so the cost analysis tool has something to find:
  * hourly usage rows, like the real export
  * daily cycle (busy afternoons) and weekly cycle (quiet weekends) on prod
  * gentle month-over-month growth
  * a cost anomaly: a BigQuery analysis spike in prod-data (configurable day)
  * credits as negative amounts: committed use discount, free tier, promotion
  * rows with two credits (committed use + sustained use discount on prod CPU)
  * some resources without labels (unallocated cost)

Why not the emulator's --data-from-yaml seed? In emulator v0.8.1 the YAML
loader ignores `mode: REPEATED`, so labels/credits arrays become single
structs, and streaming inserts corrupt arrays nested inside structs
(project.labels, project.ancestors). Creating the table via the API and
inserting with DML avoids both problems.

Usage:
    pip install google-cloud-bigquery

    # 1) start the emulator (no seed file needed)
    docker run -it -p 9050:9050 -p 9060:9060 \
        ghcr.io/goccy/bigquery-emulator:latest --project=billing-dev

    # 2) generate + load
    python generate_billing_seed.py --emulator http://localhost:9050 --days 30

    # or: write files for a real BigQuery dev project
    python generate_billing_seed.py --files out/ --days 90
    bq load --source_format=NEWLINE_DELIMITED_JSON \
        my-dev-project:billing.gcp_billing_export_v1_FAKE00_000000_000000 \
        out/rows.ndjson out/schema.json
"""

import argparse
import hashlib
import math
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

BILLING_ACCOUNT = "FAKE00-000000-000000"
CURRENCY = "USD"
REGION = "us-central1"

# --------------------------------------------------------------------------
# Schema (subset of the real standard export; diff against your own with
#   bq show --schema --format=prettyjson PROJECT:DATASET.TABLE
# and add any columns your tool reads)
# --------------------------------------------------------------------------
KV = [{"name": "key", "type": "STRING"}, {"name": "value", "type": "STRING"}]

SCHEMA = [
    {"name": "billing_account_id", "type": "STRING"},
    {"name": "service", "type": "STRUCT", "fields": [
        {"name": "id", "type": "STRING"},
        {"name": "description", "type": "STRING"}]},
    {"name": "sku", "type": "STRUCT", "fields": [
        {"name": "id", "type": "STRING"},
        {"name": "description", "type": "STRING"}]},
    {"name": "usage_start_time", "type": "TIMESTAMP"},
    {"name": "usage_end_time", "type": "TIMESTAMP"},
    {"name": "project", "type": "STRUCT", "fields": [
        {"name": "id", "type": "STRING"},
        {"name": "number", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "labels", "type": "STRUCT", "mode": "REPEATED", "fields": KV},
        {"name": "ancestry_numbers", "type": "STRING"},
        {"name": "ancestors", "type": "STRUCT", "mode": "REPEATED", "fields": [
            {"name": "resource_name", "type": "STRING"},
            {"name": "display_name", "type": "STRING"}]}]},
    {"name": "labels", "type": "STRUCT", "mode": "REPEATED", "fields": KV},
    {"name": "system_labels", "type": "STRUCT", "mode": "REPEATED", "fields": KV},
    {"name": "location", "type": "STRUCT", "fields": [
        {"name": "location", "type": "STRING"},
        {"name": "country", "type": "STRING"},
        {"name": "region", "type": "STRING"},
        {"name": "zone", "type": "STRING"}]},
    {"name": "export_time", "type": "TIMESTAMP"},
    {"name": "cost", "type": "FLOAT"},
    {"name": "currency", "type": "STRING"},
    {"name": "currency_conversion_rate", "type": "FLOAT"},
    {"name": "usage", "type": "STRUCT", "fields": [
        {"name": "amount", "type": "FLOAT"},
        {"name": "unit", "type": "STRING"},
        {"name": "amount_in_pricing_units", "type": "FLOAT"},
        {"name": "pricing_unit", "type": "STRING"}]},
    {"name": "credits", "type": "STRUCT", "mode": "REPEATED", "fields": [
        {"name": "name", "type": "STRING"},
        {"name": "amount", "type": "FLOAT"},
        {"name": "full_name", "type": "STRING"},
        {"name": "id", "type": "STRING"},
        {"name": "type", "type": "STRING"}]},
    {"name": "invoice", "type": "STRUCT", "fields": [
        {"name": "month", "type": "STRING"},
        {"name": "publisher_type", "type": "STRING"}]},
    {"name": "cost_type", "type": "STRING"},
    {"name": "cost_at_list", "type": "FLOAT"},
    {"name": "transaction_type", "type": "STRING"},
    {"name": "seller_name", "type": "STRING"},
]

# --------------------------------------------------------------------------
# Fake organisation
# --------------------------------------------------------------------------
ORG = {"resource_name": "organizations/111111111111", "display_name": "example.com"}
FOLDERS = {
    "production": {"resource_name": "folders/222222222221", "display_name": "production"},
    "non-production": {"resource_name": "folders/222222222222", "display_name": "non-production"},
}

PROJECTS = [
    # id, number, folder, project labels, weekly pattern?, scale
    ("acme-prod-web", "100000000001", "production",
     {"env": "prod", "team": "web"}, True, 1.0),
    ("acme-prod-data", "100000000002", "production",
     {"env": "prod", "team": "data"}, True, 1.0),
    ("acme-staging", "100000000003", "non-production",
     {"env": "staging", "team": "platform"}, False, 0.35),
    ("acme-sandbox", "100000000004", "non-production",
     {}, False, 0.15),  # deliberately unlabeled project
]

# --------------------------------------------------------------------------
# Catalog: service -> SKUs with pricing.
#   hourly_usage: typical usage per hour (in `unit`)
#   per_pricing_unit: how many `unit` make one `pricing_unit`
#   price: list price per pricing unit (USD)
#   diurnal: how strongly usage follows the time of day (0..1)
# --------------------------------------------------------------------------
GIB = 1024 ** 3
TIB = 1024 ** 4
HOURS_PER_MONTH = 730

SKUS = {
    "Compute Engine": {
        "id": "AAAA-0001-0001",
        "skus": [
            dict(id="C0E2-0001-CORE", desc="E2 Instance Core running in Americas",
                 unit="seconds", pricing_unit="hour", per_pricing_unit=3600,
                 price=0.021811, hourly_usage=16 * 3600, diurnal=0.2, zonal=True,
                 sys={"compute.googleapis.com/machine_spec": "e2-standard-4",
                      "compute.googleapis.com/cores": "4"}),
            dict(id="C0E2-0002-RAM0", desc="E2 Instance Ram running in Americas",
                 unit="byte-seconds", pricing_unit="gibibyte hour",
                 per_pricing_unit=GIB * 3600, price=0.002923,
                 hourly_usage=64 * GIB * 3600, diurnal=0.2, zonal=True,
                 sys={"compute.googleapis.com/machine_spec": "e2-standard-4",
                      "compute.googleapis.com/memory": "16384"}),
            dict(id="C0PD-0003-SSD0", desc="SSD backed PD Capacity",
                 unit="byte-seconds", pricing_unit="gibibyte month",
                 per_pricing_unit=GIB * 3600 * HOURS_PER_MONTH, price=0.17,
                 hourly_usage=500 * GIB * 3600, diurnal=0.0, zonal=True, sys={}),
        ],
    },
    "BigQuery": {
        "id": "AAAA-0002-0002",
        "skus": [
            dict(id="BQ00-0001-ANLY", desc="Analysis",
                 unit="bytes", pricing_unit="tebibyte", per_pricing_unit=TIB,
                 price=6.25, hourly_usage=0.4 * TIB, diurnal=0.8, zonal=False, sys={}),
            dict(id="BQ00-0002-ASTO", desc="Active Logical Storage",
                 unit="byte-seconds", pricing_unit="gibibyte month",
                 per_pricing_unit=GIB * 3600 * HOURS_PER_MONTH, price=0.02,
                 hourly_usage=8000 * GIB * 3600, diurnal=0.0, zonal=False, sys={}),
        ],
    },
    "Cloud Storage": {
        "id": "AAAA-0003-0003",
        "skus": [
            dict(id="GCS0-0001-STD0", desc="Standard Storage US Multi-region",
                 unit="byte-seconds", pricing_unit="gibibyte month",
                 per_pricing_unit=GIB * 3600 * HOURS_PER_MONTH, price=0.026,
                 hourly_usage=12000 * GIB * 3600, diurnal=0.0, zonal=False,
                 multi_region="US", sys={}),
        ],
    },
    "Cloud SQL": {
        "id": "AAAA-0004-0004",
        "skus": [
            dict(id="SQL0-0001-VCPU", desc="Cloud SQL for PostgreSQL: Zonal - vCPU in Americas",
                 unit="seconds", pricing_unit="hour", per_pricing_unit=3600,
                 price=0.0413, hourly_usage=8 * 3600, diurnal=0.0, zonal=True, sys={}),
        ],
    },
    "Cloud Run": {
        "id": "AAAA-0005-0005",
        "skus": [
            dict(id="RUN0-0001-CPU0", desc="CPU Allocation Time",
                 unit="seconds", pricing_unit="vCPU-second", per_pricing_unit=1,
                 price=0.000024, hourly_usage=9000, diurnal=0.9, zonal=False, sys={}),
        ],
    },
    "Networking": {
        "id": "AAAA-0006-0006",
        "skus": [
            dict(id="NET0-0001-EGRS", desc="Network Internet Data Transfer Out from Americas to Americas",
                 unit="bytes", pricing_unit="gibibyte", per_pricing_unit=GIB,
                 price=0.12, hourly_usage=6 * GIB, diurnal=0.7, zonal=False, sys={}),
        ],
    },
}

# Which services run in which project, with resource-level labels.
# (project_id, service, label dict)
WORKLOADS = [
    ("acme-prod-web", "Compute Engine", {"app": "frontend"}),
    ("acme-prod-web", "Cloud SQL", {"app": "frontend-db"}),
    ("acme-prod-web", "Cloud Run", {"app": "api"}),
    ("acme-prod-web", "Networking", {}),
    ("acme-prod-web", "Cloud Storage", {"app": "assets"}),
    ("acme-prod-data", "BigQuery", {"app": "warehouse"}),
    ("acme-prod-data", "Cloud Storage", {"app": "datalake"}),
    ("acme-prod-data", "Compute Engine", {}),  # unlabeled VMs: unallocated cost
    ("acme-staging", "Compute Engine", {"app": "frontend"}),
    ("acme-staging", "Cloud SQL", {"app": "frontend-db"}),
    ("acme-staging", "Cloud Run", {"app": "api"}),
    ("acme-sandbox", "Compute Engine", {}),
    ("acme-sandbox", "BigQuery", {}),
]


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def kv(d: dict) -> list:
    return [{"key": k, "value": v} for k, v in d.items()]


def stable_id(*parts) -> str:
    return hashlib.sha1("|".join(parts).encode()).hexdigest()[:12]


def usage_multiplier(hour_dt: datetime, start: datetime, diurnal: float,
                     weekly: bool, rng: random.Random) -> float:
    # Daily cycle: peak ~15:00 UTC, trough ~03:00 UTC.
    day = 1 + diurnal * math.sin((hour_dt.hour - 9) / 24 * 2 * math.pi)
    # Weekend dip for production workloads.
    week = 0.6 if (weekly and hour_dt.weekday() >= 5 and diurnal > 0) else 1.0
    # ~8% monthly growth.
    days_in = (hour_dt - start).total_seconds() / 86400
    growth = 1 + 0.08 * days_in / 30
    noise = rng.uniform(0.9, 1.1)
    return max(day, 0.05) * week * growth * noise


def credits_for(project_id: str, service: str, sku: dict, cost: float) -> list:
    out = []
    if project_id.startswith("acme-prod") and service == "Compute Engine" \
            and sku["unit"] == "seconds":
        out.append({
            "name": "Committed use discount: CPU",
            "amount": round(-cost * 0.37, 6),
            "full_name": "Committed use discount: CPU",
            "id": "committed-usage-discount-cpu",
            "type": "COMMITTED_USAGE_DISCOUNT",
        })
        # Second credit on the same row: real exports often stack discounts, and
        # consumers must sum credits per row instead of joining them one by one.
        out.append({
            "name": "Sustained use discount",
            "amount": round(-cost * 0.05, 6),
            "full_name": "Sustained use discount",
            "id": "sustained-usage-discount-cpu",
            "type": "SUSTAINED_USAGE_DISCOUNT",
        })
    if service == "Cloud Run":
        out.append({
            "name": "Free Tier",
            "amount": round(-min(cost, 0.02), 6),
            "full_name": "Free Tier",
            "id": "free-tier",
            "type": "FREE_TIER",
        })
    if project_id == "acme-sandbox":
        out.append({
            "name": "Startup program credit",
            "amount": round(-cost, 6),  # sandbox fully covered by promo credit
            "full_name": "Startup program credit",
            "id": "promo-startup-0001",
            "type": "PROMOTION",
        })
    return out


# --------------------------------------------------------------------------
# Row generation
# --------------------------------------------------------------------------
def build_rows(days: int, end: datetime, spike_day: int, seed: int) -> list:
    rng = random.Random(seed)
    start = (end - timedelta(days=days)).replace(minute=0, second=0, microsecond=0)
    projects = {p[0]: p for p in PROJECTS}
    rows = []

    for h in range(days * 24):
        t0 = start + timedelta(hours=h)
        t1 = t0 + timedelta(hours=1)
        export_time = t1 + timedelta(hours=rng.randint(2, 20), minutes=rng.randint(0, 59))

        for project_id, service, res_labels in WORKLOADS:
            _, number, folder, plabels, weekly, scale = projects[project_id]
            svc = SKUS[service]

            for sku in svc["skus"]:
                mult = usage_multiplier(t0, start, sku["diurnal"], weekly, rng)

                # Anomaly: runaway BigQuery job in prod-data, 6 hours long.
                if (project_id == "acme-prod-data" and sku["id"] == "BQ00-0001-ANLY"
                        and (t0 - start).days == spike_day and 10 <= t0.hour < 16):
                    mult *= 25

                amount = sku["hourly_usage"] * scale * mult
                in_pricing = amount / sku["per_pricing_unit"]
                cost = round(in_pricing * sku["price"], 6)

                if "multi_region" in sku:
                    loc = {"location": sku["multi_region"], "country": "US",
                           "region": None, "zone": None}
                else:
                    loc = {"location": REGION, "country": "US", "region": REGION,
                           "zone": f"{REGION}-a" if sku["zonal"] else None}

                rows.append({
                    "billing_account_id": BILLING_ACCOUNT,
                    "service": {"id": svc["id"], "description": service},
                    "sku": {"id": sku["id"], "description": sku["desc"]},
                    "usage_start_time": ts(t0),
                    "usage_end_time": ts(t1),
                    "project": {
                        "id": project_id,
                        "number": number,
                        "name": project_id,
                        "labels": kv(plabels),
                        "ancestry_numbers": "/{}/{}/".format(
                            ORG["resource_name"].split("/")[1],
                            FOLDERS[folder]["resource_name"].split("/")[1]),
                        "ancestors": [
                            {"resource_name": f"projects/{number}", "display_name": project_id},
                            FOLDERS[folder],
                            ORG,
                        ],
                    },
                    "labels": kv(res_labels),
                    "system_labels": kv(sku["sys"]),
                    "location": loc,
                    "export_time": ts(export_time),
                    "cost": cost,
                    "currency": CURRENCY,
                    "currency_conversion_rate": 1.0,
                    "usage": {
                        "amount": round(amount, 3),
                        "unit": sku["unit"],
                        "amount_in_pricing_units": round(in_pricing, 6),
                        "pricing_unit": sku["pricing_unit"],
                    },
                    "credits": credits_for(project_id, service, sku, cost),
                    "invoice": {"month": t0.strftime("%Y%m"), "publisher_type": "GOOGLE"},
                    "cost_type": "regular",
                    "cost_at_list": cost,
                    "transaction_type": "GOOGLE",
                    "seller_name": "Google",
                })
    return rows


# --------------------------------------------------------------------------
# Output: schema helpers
# --------------------------------------------------------------------------
SQL_TYPES = {"STRING": "STRING", "FLOAT": "FLOAT64", "INTEGER": "INT64",
             "TIMESTAMP": "TIMESTAMP", "BOOLEAN": "BOOL", "NUMERIC": "NUMERIC"}


def to_bq_json_schema(fields: list) -> list:
    """Schema in the format `bq load` / `bq show --schema` uses."""
    out = []
    for f in fields:
        col = {"name": f["name"],
               "type": "RECORD" if f["type"] == "STRUCT" else f["type"],
               "mode": f.get("mode", "NULLABLE")}
        if f.get("fields"):
            col["fields"] = to_bq_json_schema(f["fields"])
        out.append(col)
    return out


def sql_type(f: dict, include_mode: bool = True) -> str:
    if f["type"] == "STRUCT":
        inner = ", ".join(f"{x['name']} {sql_type(x)}" for x in f["fields"])
        base = f"STRUCT<{inner}>"
    else:
        base = SQL_TYPES[f["type"]]
    if include_mode and f.get("mode") == "REPEATED":
        return f"ARRAY<{base}>"
    return base


def sql_literal(value, f: dict) -> str:
    """Render a Python value as a fully-typed GoogleSQL literal."""
    if f.get("mode") == "REPEATED":
        items = ", ".join(sql_literal(v, {**f, "mode": "NULLABLE"}) for v in (value or []))
        return f"{sql_type(f)}[{items}]"
    if value is None:
        return f"CAST(NULL AS {sql_type(f)})"
    t = f["type"]
    if t == "STRUCT":
        parts = ", ".join(sql_literal(value.get(x["name"]), x) for x in f["fields"])
        return f"{sql_type(f)}({parts})"
    if t == "STRING":
        return json.dumps(value)  # double-quoted, escapes are valid GoogleSQL
    if t == "TIMESTAMP":
        return f"TIMESTAMP '{value}+00'"
    if t == "BOOLEAN":
        return "TRUE" if value else "FALSE"
    return repr(value)


# --------------------------------------------------------------------------
# Output: real BigQuery (files)
# --------------------------------------------------------------------------
def write_files(rows: list, out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "schema.json"), "w") as f:
        json.dump(to_bq_json_schema(SCHEMA), f, indent=2)
    with open(os.path.join(out_dir, "rows.ndjson"), "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    print(f"Wrote {out_dir}/schema.json and {out_dir}/rows.ndjson")


# --------------------------------------------------------------------------
# Output: local emulator
# --------------------------------------------------------------------------
def load_emulator(rows: list, endpoint: str, project: str, dataset: str,
                  table: str, batch: int) -> None:
    from google.api_core.client_options import ClientOptions
    from google.api_core.exceptions import NotFound
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    client = bigquery.Client(project=project,
                             client_options=ClientOptions(api_endpoint=endpoint),
                             credentials=AnonymousCredentials())

    # The emulator answers "already exists" with HTTP 500, which the client
    # retries forever, so check explicitly instead of using exists_ok=True.
    try:
        client.get_dataset(f"{project}.{dataset}")
    except NotFound:
        client.create_dataset(f"{project}.{dataset}")

    table_id = f"{project}.{dataset}.{table}"
    client.delete_table(table_id, not_found_ok=True)
    columns = ",\n  ".join(f"{f['name']} {sql_type(f)}" for f in SCHEMA)
    client.query(f"CREATE TABLE `{table_id}` (\n  {columns}\n)").result()

    names = ", ".join(f["name"] for f in SCHEMA)
    started = time.time()
    for i in range(0, len(rows), batch):
        chunk = rows[i:i + batch]
        values = ",\n".join(
            "(" + ", ".join(sql_literal(r.get(f["name"]), f) for f in SCHEMA) + ")"
            for r in chunk)
        client.query(f"INSERT INTO `{table_id}` ({names}) VALUES\n{values}").result()
        done = i + len(chunk)
        print(f"\r  inserted {done:,}/{len(rows):,} rows "
              f"({time.time() - started:.0f}s)", end="", flush=True)
    print(f"\nLoaded into {table_id} at {endpoint}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    dest = ap.add_mutually_exclusive_group(required=True)
    dest.add_argument("--emulator", metavar="URL",
                      help="load into a running emulator, e.g. http://localhost:9050")
    dest.add_argument("--files", metavar="DIR",
                      help="write schema.json + rows.ndjson for `bq load`")
    ap.add_argument("--project", default="billing-dev",
                    help="emulator project id (must match --project on the emulator)")
    ap.add_argument("--dataset", default="billing")
    ap.add_argument("--table", default="gcp_billing_export_v1_FAKE00_000000_000000")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--end", default=None,
                    help="end date YYYY-MM-DD, exclusive (default: today, UTC)")
    ap.add_argument("--spike-day", type=int, default=None,
                    help="day index (0-based) of the BigQuery cost anomaly "
                         "(default: two-thirds of the way through)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--batch", type=int, default=250,
                    help="rows per INSERT statement when loading the emulator")
    args = ap.parse_args()

    end = (datetime.strptime(args.end, "%Y-%m-%d") if args.end
           else datetime.now(timezone.utc).replace(tzinfo=None))
    end = end.replace(hour=0, minute=0, second=0, microsecond=0)
    spike_day = args.spike_day if args.spike_day is not None else (args.days * 2) // 3

    rows = build_rows(args.days, end, spike_day, args.seed)
    total = sum(r["cost"] for r in rows)
    print(f"Generated {len(rows):,} rows, gross cost ${total:,.2f}, "
          f"anomaly on {(end - timedelta(days=args.days - spike_day)).date()}")

    if args.files:
        write_files(rows, args.files)
    else:
        load_emulator(rows, args.emulator, args.project, args.dataset,
                      args.table, args.batch)


if __name__ == "__main__":
    main()
