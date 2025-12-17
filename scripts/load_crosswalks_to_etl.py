"""
Load crosswalk CSV assets (e.g., d_items_to_concept) into the ETL database for review/reporting.

These crosswalks are not required for the core ETL to run, but are useful for
auditing and mapping gap analysis.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load crosswalk CSVs into the ETL DuckDB")
    parser.add_argument("--database", required=True, help="DuckDB database to write into")
    parser.add_argument("--etl-schema", default="omop_cdm", help="ETL schema (default: omop_cdm)")
    parser.add_argument("--crosswalk-dir", default="crosswalk_csv", help="Folder containing crosswalk CSVs")
    return parser.parse_args()


def load_one(con: duckdb.DuckDBPyConnection, etl_schema: str, csv_path: Path, table_name: str) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {etl_schema}")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {etl_schema}.{table_name} AS
        SELECT *
        FROM read_csv_auto('{csv_path.as_posix()}', HEADER=TRUE, DELIM=',', QUOTE='\"', ESCAPE='\"')
        """
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.database)
    crosswalk_dir = Path(args.crosswalk_dir)
    con = duckdb.connect(str(db_path))
    try:
        d_items = crosswalk_dir / "d_items_to_concept.csv"
        if d_items.exists():
            load_one(con, args.etl_schema, d_items, "crosswalk_d_items_to_concept")
            print(f"Loaded {d_items} into {args.etl_schema}.crosswalk_d_items_to_concept")
        else:
            print(f"Crosswalk file not found: {d_items} (skipping)")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

