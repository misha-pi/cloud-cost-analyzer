"""
Made-up AWS Cost Explorer data in viz_rill/data/aws_costs (gitignored) for the demo: the last 13 months up
to yesterday, in the files the real loader writes (pipelines/aws_cost_explorer_pipeline.py). No AWS key
needed. Refuses to replace AWS data loaded by `make run-aws` unless FORCE=1.

    uv run python scripts/aws_demo_data.py
"""
import datetime
import pathlib
import random
import subprocess
import sys

import pyarrow as pa

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "pipelines"))
import aws_cost_explorer_pipeline as aws  # noqa: E402

ROOT = pathlib.Path("viz_rill/data/aws_costs")

# (service, daily cost, account, region, usage type, usage unit, tag value)
SERVICES = [
    ("Amazon CloudFront", 190.0, "Example-Main", "global", "US-DataTransfer-Out-Bytes", "GB", "web"),
    ("Amazon Simple Storage Service", 140.0, "Example-Main", "us-east-1", "TimedStorage-ByteHrs", "GB-Mo", "media"),
    ("Amazon Route 53", 14.0, "Example-Main", "global", "DNS-Queries", "Queries", "web"),
    ("Amazon Elastic Container Service for Kubernetes", 14.4, "Example-Backend", "us-east-1", "AmazonEKS-Hours:perCluster", "Hrs", "backend"),
    ("EC2 - Other", 7.0, "Example-Backend", "us-east-1", "EBS:VolumeUsage.gp3", "GB-Mo", "backend"),
    ("Amazon Relational Database Service", 5.3, "Example-Backend", "us-east-1", "InstanceUsage:db.t4g.medium", "Hrs", "backend"),
    ("Amazon Elastic Compute Cloud - Compute", 4.1, "Example-Backend", "us-east-1", "BoxUsage:t3.large", "Hrs", "backend"),
    ("Amazon Virtual Private Cloud", 1.8, "Example-Backend", "us-east-1", "PublicIPv4:InUseAddress", "Hrs", ""),
    ("AmazonCloudWatch", 0.6, "Example-Backend", "us-east-1", "CW:MetricMonitorUsage", "Metrics", ""),
    ("Amazon Simple Storage Service", 0.03, "Example-Log-Archive", "us-east-1", "TimedStorage-ByteHrs", "GB-Mo", ""),
]
ACCOUNT_IDS = {"Example-Main": "100000000001", "Example-Backend": "100000000002", "Example-Log-Archive": "100000000003"}
MARKETPLACE = ("Vercel", 60000.0, "Example-Main", "global", "Subscription", "Units", "web")


def rows_for(day: datetime.date, rng: random.Random, estimated: bool):
    trend = 1 + 0.02 * ((day.year * 12 + day.month) % 12)  # gentle growth through the year
    items = [(s, base * trend * rng.uniform(0.85, 1.15), *rest) for s, base, *rest in SERVICES]
    if day.day == 1 and day.month == 10:  # one yearly Marketplace contract
        items.append(MARKETPLACE)
    for service, cost, account, region, usage_type, unit, tag in items:
        common = {"date": day, "provider": "Vercel (via AWS)" if service == "Vercel" else "AWS",
                  "service": service, "cost": round(cost, 4), "net_cost": round(cost, 4), "estimated": estimated}
        yield "by_account", common | {"account_id": ACCOUNT_IDS[account], "account_name": account}
        yield "by_region", common | {"region": region}
        yield "by_usage_type", common | {"usage_type": usage_type, "usage_quantity": round(cost * 7.3, 2), "usage_unit": unit}
        yield "by_tag", common | {"tag_key": "app", "tag_value": tag or "(no tag)"}


def main():
    subprocess.run(["sh", "scripts/check_no_real_data.sh", str(ROOT)], check=True)
    rng = random.Random(42)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    if ROOT.exists():
        import shutil
        shutil.rmtree(ROOT)
    for month in aws.history(datetime.date.today()):
        tables = {name: [] for name in aws.SCHEMAS}
        day = month
        while day.month == month.month and day <= yesterday:
            for table, row in rows_for(day, rng, estimated=(yesterday - day).days < 3):
                tables[table].append(row)
            day += datetime.timedelta(days=1)
        aws.write_month(ROOT, {n: pa.Table.from_pylist(r, schema=aws.SCHEMAS[n]) for n, r in tables.items()}, month)
    (ROOT / "_demo_data").touch()
    print(f"AWS demo data written to {ROOT} ({aws.HISTORY_MONTHS} months)")


if __name__ == "__main__":
    main()
