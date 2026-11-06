"""
GCP 12-month cost estimates for every SKU as a CSV, in the layout of the GCP estimate sheet: the All GCP row,
then one row per SKU with non-zero net cost, ranked by cost, and a total row.

Same rules as the GCP Spend Report page (models/gcp_forecast_estimates.sql), but over all complete days in
the daily billing data instead of complete Monday-Sunday weeks:
  Average week                     = average day x 7
  Estimated cost, past 12 months   = average week x 52
  Projected cost, next 12 months   = past 12 months x 1.1
  NK suggested                     = 40% of the projection
Net cost after credits, all GCP projects; MongoDB Atlas bought through GCP is left out (as in gcp_costs).
A day is complete from the first export day to the day before the latest export day. Older months are only
stored as monthly totals, so they are not used.

Reads the gcp_costs model from a running Rill (on costgenie, or through the SSH tunnel to it):
  uv run python scripts/gcp_sku_estimate_csv.py --out gcp_sku_estimates.csv
  uv run python scripts/gcp_sku_estimate_csv.py --rill http://localhost:9009 --out ~/Downloads/x.csv
"""
import argparse
import csv
import datetime
import json
import pathlib
import urllib.request

WEEKS_PER_YEAR = 52
GROWTH = 1.1
NK_SHARE = 0.4

BOUNDS_SQL = """
SELECT
  CAST(GREATEST(MIN(date), CAST(MIN(first_export_time) AS DATE)) AS VARCHAR) AS first_day,
  CAST(LEAST(MAX(date), CAST(MAX(export_time) AS DATE) - 1) AS VARCHAR) AS last_day
FROM gcp_costs
WHERE grain = 'day'
"""

SKU_SQL = """
SELECT ANY_VALUE(service_name || ' / ' || sku_description) AS sku, SUM(net_cost) AS net_cost
FROM gcp_costs
WHERE grain = 'day' AND date BETWEEN DATE '{first}' AND DATE '{last}'
GROUP BY sku__id
HAVING ROUND(SUM(net_cost), 2) <> 0
ORDER BY net_cost DESC, sku
"""


def query(rill: str, sql: str) -> list:
    request = urllib.request.Request(
        f"{rill.rstrip('/')}/v1/instances/default/query",
        data=json.dumps({"sql": sql}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request) as response:
        return json.load(response)["data"]


def estimates(net_cost: float, days: int) -> list:
    """[NK suggested, average week, past 12 months, next 12 months], rounded to cents."""
    week = net_cost / days * 7
    past = week * WEEKS_PER_YEAR
    projected = past * GROWTH
    return [round(v, 2) for v in (projected * NK_SHARE, week, past, projected)]


def rows(first: str, last: str, days: int, skus: list) -> list:
    """The CSV rows, in the layout of the GCP estimate sheet."""
    total = sum(s["net_cost"] for s in skus)
    nk, week, past, projected = estimates(total, days)
    out = [
        ["Total GCP cost (net of credits, all GCP projects)"],
        ["Days used", f"{first} to {last} ({days} days, {days / 7:.1f} weeks)"],
        [],
        ["", "NK suggested", "", "Average week", "Estimated cost, past 12 months", "Projected cost, next 12 months"],
        ["All GCP", nk, "", week, past, projected],
        [],
        ["All SKUs: cost estimates"],
        ["Rank", "SKU", "RS Edited", "NK suggested", "Average week", "Estimated cost, past 12 months",
         "Projected cost, next 12 months"],
    ]
    for rank, s in enumerate(skus, 1):
        nk_s, week_s, past_s, projected_s = estimates(s["net_cost"], days)
        out.append([rank, s["sku"], "", nk_s, week_s, past_s, projected_s])
    out.append(["", f"Total (all {len(skus)} SKUs)", "", nk, week, past, projected])
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rill", default="http://localhost:9009", help="Rill URL (default: %(default)s)")
    parser.add_argument("--out", required=True, help="CSV file to write")
    args = parser.parse_args()

    bounds = query(args.rill, BOUNDS_SQL)[0]
    first, last = bounds["first_day"], bounds["last_day"]
    days = (datetime.date.fromisoformat(last) - datetime.date.fromisoformat(first)).days + 1
    skus = query(args.rill, SKU_SQL.format(first=first, last=last))

    out = pathlib.Path(args.out).expanduser()
    with out.open("w", newline="") as f:
        csv.writer(f).writerows(rows(first, last, days, skus))
    print(f"{len(skus)} SKUs, {first} to {last} ({days} days), net cost {sum(s['net_cost'] for s in skus):,.2f} USD -> {out}")


if __name__ == "__main__":
    main()
