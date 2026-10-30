"""
GCP billing data on disk for the Rill dashboards (filesystem destination).

    viz_rill/data/gcp_costs/
      hourly/last_24h.parquet    billing rows exported in the last 24 hours, one row per billing row
      daily/YYYY-MM/data.parquet cost per usage day, for the latest DAILY_MONTHS months
      monthly/YYYY-MM.parquet    cost per usage month, for older months

Daily and monthly files have the same columns: `grain` ('day' or 'month'), `date` (the day, or the 1st of
the month), the dimensions the dashboards group by (DIMENSIONS), summed measures (MEASURES: cost, credits, net cost, usage), the
export_time range of the rows summed and `row_count`, the number of billing rows summed.
Totals per month do not change when days are rolled up into a month.

The incremental cursor is the latest export_time in these files, so there is no separate state to keep
in sync with the data.
"""
import os
import pathlib
import shutil

import duckdb

# Billing export fields kept for each row, as (column, path in the export). Labels and credits are
# turned into columns by the query (see flat_select_list).
FIELDS = [
    ("billing_account_id", "billing_account_id"),
    ("service__id", "service.id"),
    ("service__description", "service.description"),
    ("sku__id", "sku.id"),
    ("sku__description", "sku.description"),
    ("project__id", "project.id"),
    ("project__number", "project.number"),
    ("project__name", "project.name"),
    ("location__location", "location.location"),
    ("location__country", "location.country"),
    ("location__region", "location.region"),
    ("resource__name", "resource.name"),
    ("resource__global_name", "resource.global_name"),
    ("transaction_type", "transaction_type"),
    ("seller_name", "seller_name"),
    ("cost_type", "cost_type"),
    ("currency", "currency"),
    ("usage__unit", "usage.unit"),
    ("usage__pricing_unit", "usage.pricing_unit"),
    ("invoice__month", "invoice.month"),
    ("usage_start_time", "usage_start_time"),
    ("export_time", "export_time"),
    ("cost", "cost"),
    ("cost_at_list", "cost_at_list"),
    ("usage__amount", "usage.amount"),
    ("usage__amount_in_pricing_units", "usage.amount_in_pricing_units"),
]
FIELD_TYPES = {
    "usage_start_time": "TIMESTAMP",
    "export_time": "TIMESTAMP",
    "cost": "FLOAT64",
    "cost_at_list": "FLOAT64",
    "usage__amount": "FLOAT64",
    "usage__amount_in_pricing_units": "FLOAT64",
}

# Label key -> column. These are the labels the Rill models read.
LABEL_COLUMNS = {"k8s-namespace": "k8s_namespace", "k8s-workload-name": "k8s_workload", "app": "app_label"}

# net_cost = cost + credits_total (credits are negative), stored so readers of the files need not work it out.
MEASURES = ["cost", "credits_total", "net_cost", "cost_at_list", "usage__amount", "usage__amount_in_pricing_units"]
_NOT_DIMENSIONS = set(MEASURES) | {"usage_start_time", "export_time"}
DIMENSIONS = [name for name, _ in FIELDS if name not in _NOT_DIMENSIONS] + list(LABEL_COLUMNS.values())

# Months kept per day; older months are kept per month.
DAILY_MONTHS = 3
# Billing rows kept one by one, counted back from the latest export_time.
HOURLY_WINDOW = "24 HOURS"


def _sql_string(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def flat_select_list(schema) -> str:
    """
    BigQuery SELECT list giving one flat row per billing row with the FIELDS columns, a column per
    LABEL_COLUMNS label and credits_total (the sum of the row's credits, negative). Fields missing from
    the table (the standard export has no resource) are NULL.
    """
    fields = {}
    for field in schema:
        fields[field.name] = field
        for sub in field.fields or ():
            fields[f"{field.name}.{sub.name}"] = sub
    columns = []
    for name, path in FIELDS:
        if path in fields:
            columns.append(f"{path} AS {name}")
        else:
            columns.append(f"CAST(NULL AS {FIELD_TYPES.get(name, 'STRING')}) AS {name}")
    for key, name in LABEL_COLUMNS.items():
        if "labels" in fields:
            columns.append(f"(SELECT ANY_VALUE(l.value) FROM UNNEST(labels) AS l WHERE l.key = {_sql_string(key)}) AS {name}")
        else:
            columns.append(f"CAST(NULL AS STRING) AS {name}")
    if "credits" in fields:
        columns.append("IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) AS c), 0) AS credits_total")
    else:
        columns.append("CAST(0 AS FLOAT64) AS credits_total")
    return ",\n                  ".join(columns)


def _rollup_sql(source: str, date_expr: str, grain: str) -> str:
    """Sum `source` rows per `date_expr` and DIMENSIONS. `source` has daily/monthly file columns."""
    sums = ", ".join(f"SUM({m}) AS {m}" for m in MEASURES)
    return (
        f"SELECT '{grain}' AS grain, CAST({date_expr} AS DATE) AS date, {', '.join(DIMENSIONS)}, {sums}, "
        "MIN(first_export_time) AS first_export_time, MAX(export_time) AS export_time, "
        f"CAST(SUM(row_count) AS BIGINT) AS row_count FROM ({source}) GROUP BY ALL"
    )


def _read(files) -> str:
    paths = ", ".join(_sql_string(str(f)) for f in files)
    return f"SELECT * FROM read_parquet([{paths}], union_by_name = true)"


class Store:
    """The GCP data folder (viz_rill/data/gcp_costs)."""

    def __init__(self, root):
        self.root = pathlib.Path(root)
        # Next to the data folder, so Rill never reads half-written files.
        self.work_dir = self.root.parent / f".{self.root.name}_work"
        # DuckDB's database file and spill files while adding rows, so a large add (e.g. the one-off
        # migration of millions of rows) stays within the memory limit.
        self.spill_dir = self.root.parent / f".{self.root.name}_spill"

    def clear_work(self) -> None:
        """Remove what an interrupted load left in the work folder (its rows were never added)."""
        shutil.rmtree(self.work_dir, ignore_errors=True)
        shutil.rmtree(self.spill_dir, ignore_errors=True)

    def _connect(self, on_disk=False):
        """A DuckDB connection; with on_disk, its tables live in a file under spill_dir instead of memory."""
        self.spill_dir.mkdir(parents=True, exist_ok=True)  # DuckDB does not create missing parent folders
        con = duckdb.connect(str(self.spill_dir / "work.duckdb") if on_disk else ":memory:")
        # The server has 4 GB: let DuckDB spill to disk rather than use more.
        con.execute(f"SET memory_limit = {_sql_string(os.getenv('GCP_STORAGE_MEMORY_LIMIT', '1GB'))}")
        con.execute("SET threads = 2")
        con.execute("SET preserve_insertion_order = false")
        con.execute(f"SET temp_directory = {_sql_string(str(self.spill_dir / 'tmp'))}")
        con.execute("SET TimeZone = 'UTC'")  # usage days are UTC days, as in the billing export
        return con

    def _daily_files(self, month="*"):
        return sorted(self.root.glob(f"daily/{month}/*.parquet"))

    def _monthly_files(self):
        return sorted(self.root.glob("monthly/*.parquet"))

    def _hourly_file(self):
        return self.root / "hourly" / "last_24h.parquet"

    def export_time_range(self):
        """(earliest, latest) export_time of the stored rows, or None when there are none."""
        files = self._daily_files() + self._monthly_files()
        if not files:
            return None
        con = self._connect()
        try:
            earliest, latest = con.execute(
                f"SELECT MIN(first_export_time), MAX(export_time) FROM ({_read(files)})"
            ).fetchone()
        finally:
            con.close()
        return None if latest is None else (earliest, latest)

    def new_staging_file(self, name: str) -> pathlib.Path:
        """Path for a parquet file of new flat billing rows (flat_select_list columns), added by `add`."""
        staging = self.work_dir / "staging"
        staging.mkdir(parents=True, exist_ok=True)
        return staging / f"{name}.parquet"

    def add(self, source_sql: str = None) -> int:
        """
        Add new flat billing rows: the staging files, or `source_sql` (a query with the same columns).
        Sums them into the daily files, updates the hourly file, rolls months older than DAILY_MONTHS
        into monthly files, then removes the staging files. Returns the number of billing rows added.
        """
        if source_sql is None:
            staged = sorted((self.work_dir / "staging").glob("*.parquet"))
            if not staged:
                return 0
            source_sql = _read(staged)
        con = self._connect(on_disk=True)
        try:
            con.execute(f"CREATE TABLE new_rows AS SELECT *, cost + credits_total AS net_cost FROM ({source_sql})")
            added = con.execute("SELECT COUNT(*) FROM new_rows").fetchone()[0]
            if added:
                self._add_daily(con)
                self._update_hourly(con)
            self._roll_up_months(con)
        finally:
            con.close()
        self.clear_work()
        return added

    def _write(self, con, query: str, path: pathlib.Path) -> None:
        """Write `query` to `path` through a temporary file, so a crash never leaves half a file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.tmp")
        con.execute(f"COPY ({query}) TO {_sql_string(str(tmp))} (FORMAT parquet, COMPRESSION zstd)")
        os.replace(tmp, path)

    def _add_daily(self, con) -> None:
        raw = "SELECT *, export_time AS first_export_time, 1 AS row_count FROM new_rows"
        con.execute(f"CREATE TABLE new_daily AS {_rollup_sql(raw, 'usage_start_time', 'day')}")
        months = [m for (m,) in con.execute(
            "SELECT DISTINCT strftime(date, '%Y-%m') FROM new_daily ORDER BY 1"
        ).fetchall()]
        # Write every month first, then swap them in, so an interrupted load leaves the old files.
        written = []
        for month in months:
            old = self._daily_files(month)
            source = f"SELECT * FROM new_daily WHERE strftime(date, '%Y-%m') = '{month}'"
            if old:
                source += f" UNION ALL BY NAME {_read(old)}"
            tmp = self.work_dir / "daily" / month / "data.parquet"
            self._write(con, _rollup_sql(source, "date", "day"), tmp)
            written.append((month, tmp, old))
        for month, tmp, old in written:
            target = self.root / "daily" / month / "data.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp, target)
            for f in old:
                if f != target:
                    f.unlink()

    def _update_hourly(self, con) -> None:
        hourly = self._hourly_file()
        source = "SELECT * FROM new_rows"
        if hourly.exists():
            source += f" UNION ALL BY NAME {_read([hourly])}"
        query = (
            f"SELECT * FROM ({source}) "
            f"WHERE export_time > (SELECT MAX(export_time) FROM ({source})) - INTERVAL {HOURLY_WINDOW}"
        )
        self._write(con, query, hourly)

    def _roll_up_months(self, con) -> None:
        months = sorted(p.name for p in (self.root / "daily").glob("*") if p.is_dir())
        if not months:
            return
        year, month = map(int, months[-1].split("-"))
        index = year * 12 + month - 1 - (DAILY_MONTHS - 1)
        first_daily = f"{index // 12:04d}-{index % 12 + 1:02d}"
        for old_month in (m for m in months if m < first_daily):
            source_files = self._daily_files(old_month)
            monthly = self.root / "monthly" / f"{old_month}.parquet"
            if monthly.exists():
                source_files.append(monthly)
            if source_files:
                self._write(con, _rollup_sql(_read(source_files), "date_trunc('month', date)", "month"), monthly)
            shutil.rmtree(self.root / "daily" / old_month)
            print(f"Rolled {old_month} up into {monthly.relative_to(self.root)}", flush=True)
