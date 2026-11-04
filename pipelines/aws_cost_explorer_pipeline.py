"""
AWS cost from the Cost Explorer API (GetCostAndUsage) into month files for the Rill dashboards.

    viz_rill/data/aws_costs/
      by_account/YYYY-MM.parquet     cost per day, service and linked account
      by_region/YYYY-MM.parquet      cost per day, service and region
      by_usage_type/YYYY-MM.parquet  cost and usage per day, service and usage type
      by_tag/YYYY-MM.parquet         cost per day, service and tag value, for each key in tag_keys

Each table adds up to the same daily total. Cost Explorer already returns daily totals, so the files are
small (a few MB a year) and kept as they are, with no roll-up.

A month is downloaded again and its files replaced when it is the current month, when it has no file
yet, or when its file still has days AWS marks as estimated (the previous month, until AWS finalises it).
Months older than Cost Explorer's history (HISTORY_MONTHS) are never touched.

Credentials: [sources.aws] aws_access_key_id, aws_secret_access_key in .dlt/secrets.toml. The key only
needs ce:GetCostAndUsage. Each request costs $0.01.
"""
import datetime
import os
import pathlib
import shutil

import dlt
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

# Cost Explorer keeps the current month and the 12 before it.
HISTORY_MONTHS = 13

# Marketplace products billed through AWS, shown as their own provider (like Atlas billed through GCP).
# A service matches when its name is the product name or starts with it ("Vercel Frontend Cloud").
DEFAULT_MARKETPLACE_SERVICES = ["Vercel"]

COST_METRICS = ["UnblendedCost", "NetUnblendedCost"]

COMMON = [
    ("date", pa.date32()),
    ("provider", pa.string()),
    ("service", pa.string()),
]
COSTS = [
    ("cost", pa.float64()),
    ("net_cost", pa.float64()),
    ("estimated", pa.bool_()),
]
SCHEMAS = {
    "by_account": pa.schema(COMMON + [("account_id", pa.string()), ("account_name", pa.string())] + COSTS),
    "by_region": pa.schema(COMMON + [("region", pa.string())] + COSTS),
    "by_usage_type": pa.schema(
        COMMON + [("usage_type", pa.string()), ("usage_quantity", pa.float64()), ("usage_unit", pa.string())] + COSTS
    ),
    "by_tag": pa.schema(COMMON + [("tag_key", pa.string()), ("tag_value", pa.string())] + COSTS),
}
# Second GroupBy of each table (the first is always SERVICE); by_tag uses one TAG group per key.
GROUP_BY = {"by_account": "LINKED_ACCOUNT", "by_region": "REGION", "by_usage_type": "USAGE_TYPE"}


def month_start(day: datetime.date) -> datetime.date:
    return day.replace(day=1)


def add_months(month: datetime.date, n: int) -> datetime.date:
    index = month.year * 12 + month.month - 1 + n
    return datetime.date(index // 12, index % 12 + 1, 1)


def history(today: datetime.date):
    """The months Cost Explorer can return, oldest first."""
    current = month_start(today)
    return [add_months(current, -n) for n in range(HISTORY_MONTHS - 1, -1, -1)]


def month_file(root: pathlib.Path, table: str, month: datetime.date) -> pathlib.Path:
    return root / table / f"{month:%Y-%m}.parquet"


def months_to_download(root: pathlib.Path, tables, today: datetime.date):
    """Months whose files must be (re)written: the current month, missing months and estimated months."""
    current = month_start(today)
    months = []
    for month in history(today):
        files = [month_file(root, t, month) for t in tables]
        if month == current or not all(f.exists() for f in files):
            months.append(month)
        elif any(pc.any(pq.read_table(f, columns=["estimated"]).column("estimated")).as_py() for f in files):
            months.append(month)
    return months


class CostExplorer:
    """GetCostAndUsage with paging. `client` is a boto3 Cost Explorer client."""

    def __init__(self, client, marketplace_services=None):
        self.client = client
        self.marketplace = set(DEFAULT_MARKETPLACE_SERVICES if marketplace_services is None else marketplace_services)
        self.requests = 0

    def provider(self, service: str) -> str:
        """"<product> (via AWS)" for a Marketplace product (its name, or its name and more, e.g. "Vercel
        Frontend Cloud"), else "AWS"."""
        for product in self.marketplace:
            if service == product or service.startswith(product + " "):
                return f"{product} (via AWS)"
        return "AWS"

    def _query(self, start, end, group_by, metrics):
        """Yield (day, estimated, keys, metrics, attributes) for each group, over all pages."""
        token = None
        while True:
            args = dict(
                TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
                Granularity="DAILY",
                Metrics=metrics,
                GroupBy=group_by,
            )
            if token:
                args["NextPageToken"] = token
            response = self.client.get_cost_and_usage(**args)
            self.requests += 1
            attributes = {d["Value"]: d.get("Attributes", {}) for d in response.get("DimensionValueAttributes", [])}
            for period in response["ResultsByTime"]:
                day = datetime.date.fromisoformat(period["TimePeriod"]["Start"])
                for group in period.get("Groups", []):
                    yield day, period.get("Estimated", False), group["Keys"], group["Metrics"], attributes
            token = response.get("NextPageToken")
            if not token:
                return

    def _row(self, day, estimated, service, metrics):
        return {
            "date": day,
            "provider": self.provider(service),
            "service": service,
            "cost": float(metrics["UnblendedCost"]["Amount"]),
            "net_cost": float(metrics["NetUnblendedCost"]["Amount"]),
            "estimated": bool(estimated),
        }

    def table(self, name, start, end, tag_keys=()):
        """Rows of one table for start <= date < end, as a pyarrow Table with SCHEMAS[name]."""
        rows = []
        if name == "by_tag":
            for key in tag_keys:
                group_by = [{"Type": "DIMENSION", "Key": "SERVICE"}, {"Type": "TAG", "Key": key}]
                for day, estimated, keys, metrics, _ in self._query(start, end, group_by, COST_METRICS):
                    # Tag groups come back as "key$value"; "key$" is cost without that tag.
                    value = keys[1].split("$", 1)[1] if "$" in keys[1] else keys[1]
                    rows.append(self._row(day, estimated, keys[0], metrics) | {"tag_key": key, "tag_value": value or "(no tag)"})
        else:
            dimension = GROUP_BY[name]
            metrics = COST_METRICS + (["UsageQuantity"] if name == "by_usage_type" else [])
            group_by = [{"Type": "DIMENSION", "Key": "SERVICE"}, {"Type": "DIMENSION", "Key": dimension}]
            for day, estimated, keys, values, attributes in self._query(start, end, group_by, metrics):
                row = self._row(day, estimated, keys[0], values)
                if name == "by_account":
                    row |= {"account_id": keys[1], "account_name": attributes.get(keys[1], {}).get("description", keys[1])}
                elif name == "by_region":
                    row["region"] = keys[1] or "global"
                else:
                    usage = values["UsageQuantity"]
                    row |= {"usage_type": keys[1], "usage_quantity": float(usage["Amount"]), "usage_unit": usage.get("Unit")}
                rows.append(row)
        return pa.Table.from_pylist(rows, schema=SCHEMAS[name])


def write_month(root: pathlib.Path, tables: dict, month: datetime.date) -> None:
    """Write one month's tables through temporary files, then swap them all in."""
    written = []
    for name, table in tables.items():
        target = month_file(root, name, month)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_name(f".{target.name}.tmp")
        pq.write_table(table, tmp, compression="zstd")
        written.append((tmp, target))
    for tmp, target in written:
        os.replace(tmp, target)


def load(root: pathlib.Path, explorer: CostExplorer, today: datetime.date, tag_keys=()) -> list:
    """Download the months that need it and replace their files. Returns the months written."""
    tables = list(SCHEMAS)
    months = months_to_download(root, tables, today)
    for month in months:
        # Up to yesterday: today's cost is still coming in. On the 1st the current month has no days yet.
        start, end = month, min(add_months(month, 1), today)
        if start < end:
            data = {name: explorer.table(name, start, end, tag_keys) for name in tables}
        else:
            data = {name: SCHEMAS[name].empty_table() for name in tables}
        write_month(root, data, month)
        total = pc.sum(data["by_account"].column("net_cost")).as_py() or 0
        print(f"AWS {month:%Y-%m}: {data['by_account'].num_rows:,} account rows, net cost {total:,.2f} USD", flush=True)
    return months


def remove_demo_data(root: pathlib.Path) -> None:
    """`make demo` marks its data with `_demo_data`; delete it before loading real data."""
    if (root / "_demo_data").exists():
        print(f"Removing demo data in {root} before loading real data", flush=True)
        shutil.rmtree(root)


def write_empty_months(root: pathlib.Path, today: datetime.date) -> None:
    """Empty files for months without one, so the AWS models (and the pages that use them) still build."""
    for month in history(today):
        if not all(month_file(root, t, month).exists() for t in SCHEMAS):
            write_month(root, {name: schema.empty_table() for name, schema in SCHEMAS.items()}, month)


def main() -> None:
    import boto3

    root = pathlib.Path(dlt.config["destination.filesystem.bucket_url"]) / "aws_costs"
    today = datetime.datetime.now(datetime.timezone.utc).date()
    key_id = dlt.secrets.get("sources.aws.aws_access_key_id")
    secret = dlt.secrets.get("sources.aws.aws_secret_access_key")
    if not key_id or not secret:
        print("No AWS key in .dlt/secrets.toml [sources.aws]: AWS not loaded, empty AWS files kept so the pages build")
        if not (root / "_demo_data").exists():
            write_empty_months(root, today)
        return
    remove_demo_data(root)
    client = boto3.client(
        "ce",
        aws_access_key_id=key_id,
        aws_secret_access_key=secret,
        region_name=dlt.config.get("sources.aws.region_name") or "us-east-1",
    )
    tag_keys = dlt.config.get("sources.aws.tag_keys") or []
    marketplace = dlt.config.get("sources.aws.marketplace_services")
    explorer = CostExplorer(client, marketplace)
    months = load(root, explorer, today, tag_keys)
    print(f"Loaded {len(months)} AWS month(s) into {root} with {explorer.requests} Cost Explorer requests "
          f"(about ${explorer.requests * 0.01:.2f})")


if __name__ == "__main__":
    main()
