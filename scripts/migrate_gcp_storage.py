"""
Move GCP billing data from the old dlt layout (one file per load in bigquery_billing_table/, with labels
and credits in child tables) to the day and month files of pipelines/gcp_storage.py. Nothing is downloaded.

    uv run python scripts/migrate_gcp_storage.py viz_rill/data/gcp_costs            # migrate, keep old files
    uv run python scripts/migrate_gcp_storage.py viz_rill/data/gcp_costs --delete-old

Stop Rill first on a small server. The old files are only deleted with --delete-old, after the totals
of the old and new files have matched. Run with --delete-old after a migration without it to compare
again and delete the old files.
"""
import argparse
import pathlib
import shutil
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "pipelines"))
import gcp_storage  # noqa: E402

OLD_TABLE = "bigquery_billing_table"
OLD_DIRS = [OLD_TABLE] + [f"{OLD_TABLE}__{t}" for t in ("labels", "credits", "project__ancestors", "project__labels",
                                              "system_labels", "tags")] + ["_dlt_loads", "_dlt_pipeline_state",
            "_dlt_version", "init"]


def _glob(root, table):
    return gcp_storage._sql_string(str(root / table / "*.parquet"))


def _has_files(root, table):
    return any((root / table).glob("*.parquet"))


def old_rows_sql(root: pathlib.Path) -> str:
    """The old files as flat billing rows, with the columns gcp_storage.flat_select_list gives."""
    con = duckdb.connect()
    present = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_glob(root, OLD_TABLE)}, union_by_name = true)"
    ).fetchall()}
    con.close()
    columns = []
    for name, _ in gcp_storage.FIELDS:
        sql_type = "DOUBLE" if gcp_storage.FIELD_TYPES.get(name) == "FLOAT64" else gcp_storage.FIELD_TYPES.get(name, "VARCHAR")
        columns.append(f"b.{name}" if name in present else f"CAST(NULL AS {sql_type}) AS {name}")
    joins = []
    if _has_files(root, f"{OLD_TABLE}__labels"):
        pivots = ", ".join(
            f"MAX(CASE WHEN key = {gcp_storage._sql_string(k)} THEN value END) AS {c}"
            for k, c in gcp_storage.LABEL_COLUMNS.items()
        )
        keys = ", ".join(gcp_storage._sql_string(k) for k in gcp_storage.LABEL_COLUMNS)
        joins.append(
            f"LEFT JOIN (SELECT _dlt_parent_id, {pivots} "
            f"FROM read_parquet({_glob(root, OLD_TABLE + '__labels')}, union_by_name = true) "
            f"WHERE key IN ({keys}) GROUP BY _dlt_parent_id) l ON l._dlt_parent_id = b._dlt_id"
        )
        columns += [f"l.{c}" for c in gcp_storage.LABEL_COLUMNS.values()]
    else:
        columns += [f"CAST(NULL AS VARCHAR) AS {c}" for c in gcp_storage.LABEL_COLUMNS.values()]
    if _has_files(root, f"{OLD_TABLE}__credits"):
        joins.append(
            f"LEFT JOIN (SELECT _dlt_parent_id, SUM(amount) AS credits_total "
            f"FROM read_parquet({_glob(root, OLD_TABLE + '__credits')}, union_by_name = true) "
            f"GROUP BY _dlt_parent_id) c ON c._dlt_parent_id = b._dlt_id"
        )
        columns.append("COALESCE(c.credits_total, 0) AS credits_total")
    else:
        columns.append("CAST(0 AS DOUBLE) AS credits_total")
    return (
        f"SELECT {', '.join(columns)} FROM read_parquet({_glob(root, OLD_TABLE)}, union_by_name = true) b "
        + " ".join(joins)
    )


def totals(con, sql):
    return con.execute(
        f"SELECT strftime(CAST(usage_start_time AS DATE), '%Y-%m') AS month, COUNT(*), ROUND(SUM(cost), 2), "
        f"ROUND(SUM(credits_total), 2) FROM ({sql}) GROUP BY 1 ORDER BY 1"
    ).fetchall()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("root", type=pathlib.Path, help="the GCP data folder, e.g. viz_rill/data/gcp_costs")
    parser.add_argument("--delete-old", action="store_true", help="delete the old files once the totals match")
    args = parser.parse_args()
    root = args.root

    store = gcp_storage.Store(root)
    if not _has_files(root, OLD_TABLE):
        raise SystemExit(f"No old files in {root / OLD_TABLE}: nothing to migrate")
    migrated = store.export_time_range() is not None
    if migrated and not args.delete_old:
        raise SystemExit(f"{root} already has day or month files: migrating again would count rows twice. "
                         "Add --delete-old to compare them with the old files and delete those.")

    source = old_rows_sql(root)
    con = store._connect()
    try:
        before = totals(con, source)
    finally:
        con.close()
    store.clear_work()
    # Already migrated: only compare, then delete the old files.
    added = 0 if migrated else store.add(source)
    con = store._connect()
    try:
        files = store._daily_files() + store._monthly_files()
        after = con.execute(
            f"SELECT strftime(date, '%Y-%m'), SUM(row_count), ROUND(SUM(cost), 2), ROUND(SUM(credits_total), 2) "
            f"FROM ({gcp_storage._read(files)}) GROUP BY 1 ORDER BY 1"
        ).fetchall()
    finally:
        con.close()
    store.clear_work()

    def same(old, new):
        # Sums of the same numbers in another order can differ by a cent.
        return old[:2] == new[:2] and all(abs(a - b) <= 0.05 for a, b in zip(old[2:], new[2:]))

    if not migrated:
        print(f"Migrated {added:,} billing rows.")
    print("Per usage month (rows, cost, credits), old -> new:")
    for old, new in zip(before, after):
        print(f"  {old[0]}: {old[1:]} -> {new[1:]}{'' if same(old, new) else '  MISMATCH'}")
    if len(before) != len(after) or not all(same(o, n) for o, n in zip(before, after)):
        raise SystemExit("The old and new totals differ: the old files are kept")
    if args.delete_old:
        for name in OLD_DIRS:
            path = root / name
            if path.is_dir():
                shutil.rmtree(path)
            elif path.exists():
                path.unlink()
        print("Deleted the old files")


if __name__ == "__main__":
    main()
