"""
Optionally replace published OMOP views with materialized tables.

Some downstream tools prefer base tables over views. This script converts all
views in the given schema into tables (same names) by selecting their contents,
dropping the views, and renaming the temporary tables.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize OMOP views into tables")
    parser.add_argument("--database", required=True, help="DuckDB database containing the OMOP schema")
    parser.add_argument("--schema", default="omop", help="Schema containing published OMOP views (default: omop)")
    parser.add_argument("--enable", default="0", help="Set to 1 to materialize; otherwise no-op")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if str(args.enable).strip() not in ("1", "true", "TRUE", "yes", "YES"):
        print("Materialization disabled; skipping.")
        return 0

    db_path = Path(args.database)
    con = duckdb.connect(str(db_path))
    try:
        views = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ? AND table_type = 'VIEW'
            ORDER BY table_name
            """,
            [args.schema],
        ).fetchall()
        for (view_name,) in views:
            tmp_name = f"{view_name}__tmp_materialized"
            con.execute(f"DROP TABLE IF EXISTS {args.schema}.{tmp_name}")
            con.execute(f"CREATE TABLE {args.schema}.{tmp_name} AS SELECT * FROM {args.schema}.{view_name}")
            con.execute(f"DROP VIEW {args.schema}.{view_name}")
            con.execute(f"ALTER TABLE {args.schema}.{tmp_name} RENAME TO {view_name}")
            print(f"Materialized {args.schema}.{view_name}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

