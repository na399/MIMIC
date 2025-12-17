"""
Ensure the smoke vocabulary DuckDB contains the OMOP vocabulary tables expected by the ETL.

Some "smoke" vocab files may include only a subset of tables (e.g., concept and
concept_relationship). The ETL expects the standard OMOP vocabulary tables to
exist (even if empty). This script bootstraps missing tables by copying their
schemas from a full vocabulary DuckDB and creating empty (0-row) tables.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


REQUIRED_TABLES = (
    "concept",
    "concept_relationship",
    "vocabulary",
    "domain",
    "concept_class",
    "relationship",
    "concept_synonym",
    "concept_ancestor",
    "drug_strength",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bootstrap missing OMOP vocab tables in a smoke DuckDB")
    parser.add_argument("--smoke-db", required=True, help="Path to smoke vocab DuckDB to modify")
    parser.add_argument("--full-db", required=True, help="Path to full vocab DuckDB (schema template)")
    parser.add_argument("--schema", default="main", help="Schema name inside both DuckDB files (default: main)")
    return parser.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, catalog: str, schema: str, table: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_catalog = ? AND table_schema = ? AND table_name = ?
            LIMIT 1
            """,
            [catalog, schema, table],
        ).fetchone()
        is not None
    )


def main() -> int:
    args = parse_args()
    smoke_db = Path(args.smoke_db)
    full_db = Path(args.full_db)
    schema = args.schema

    if not smoke_db.exists():
        raise FileNotFoundError(smoke_db)
    if not full_db.exists():
        raise FileNotFoundError(full_db)

    con = duckdb.connect(str(smoke_db))
    smoke_catalog = con.execute("PRAGMA database_list").fetchone()[1]
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"ATTACH DATABASE '{full_db}' AS full_vocab (READ_ONLY)")

    for table in REQUIRED_TABLES:
        if table_exists(con, smoke_catalog, schema, table):
            continue
        # Copy schema (0 rows) from the full vocab database.
        try:
            con.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM full_vocab.{schema}.{table} LIMIT 0")
        except duckdb.CatalogException as exc:
            raise RuntimeError(f"Template DB is missing table full_vocab.{schema}.{table}") from exc
        print(f"Created empty {schema}.{table} in {smoke_db}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
