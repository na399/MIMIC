"""
Create a smaller "snapshot" OMOP vocabulary DuckDB from a full vocab DuckDB.

The snapshot keeps only the requested concept_ids (and their concept_relationship
rows) while creating empty schema-only versions of the remaining required vocab
tables. This is useful for smoke tests and CI.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


REQUIRED_VOCAB_TABLES = (
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
    parser = argparse.ArgumentParser(description="Create a snapshot vocab DuckDB")
    parser.add_argument("--full-db", required=True, help="Path to full OMOP vocab DuckDB (read-only source)")
    parser.add_argument("--out-db", required=True, help="Path to output snapshot DuckDB (will be overwritten)")
    parser.add_argument("--schema", default="main", help="Schema name inside both DBs (default: main)")
    parser.add_argument(
        "--concept-id",
        dest="concept_ids",
        action="append",
        default=[],
        help="Concept id to include (repeatable)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    full_db = Path(args.full_db)
    out_db = Path(args.out_db)
    schema = args.schema
    if not full_db.exists():
        raise FileNotFoundError(full_db)
    out_db.parent.mkdir(parents=True, exist_ok=True)
    if out_db.exists():
        out_db.unlink()

    concept_ids = sorted({int(x) for x in args.concept_ids if str(x).strip() != ""})
    if not concept_ids:
        raise ValueError("At least one --concept-id is required")

    con = duckdb.connect(str(out_db))
    try:
        con.execute(f"ATTACH DATABASE '{full_db}' AS full_vocab (READ_ONLY)")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        for table in REQUIRED_VOCAB_TABLES:
            con.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM full_vocab.{schema}.{table} LIMIT 0")

        placeholders = ", ".join(["?"] * len(concept_ids))
        con.executemany(
            f"INSERT INTO {schema}.concept SELECT * FROM full_vocab.{schema}.concept WHERE concept_id = ?",
            [(cid,) for cid in concept_ids],
        )
        con.execute(
            f"""
            INSERT INTO {schema}.concept_relationship
            SELECT *
            FROM full_vocab.{schema}.concept_relationship
            WHERE concept_id_1 IN ({placeholders}) AND concept_id_2 IN ({placeholders})
            """,
            concept_ids + concept_ids,
        )

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
