"""
Verify an OMOP vocabulary DuckDB has the tables required by this ETL.

This is a lightweight preflight utility for troubleshooting and CI.
"""

from __future__ import annotations

import argparse
import json
import sys
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
    parser = argparse.ArgumentParser(description="Verify OMOP vocab DuckDB tables exist")
    parser.add_argument("--database", required=True, help="Path to vocab DuckDB file")
    parser.add_argument("--schema", default="main", help="Schema name inside the vocab DB (default: main)")
    parser.add_argument(
        "--validate-columns",
        action="store_true",
        default=True,
        help="Validate expected column names for required vocab tables (default: enabled)",
    )
    parser.add_argument(
        "--no-validate-columns",
        dest="validate_columns",
        action="store_false",
        help="Disable column-name validation",
    )
    parser.add_argument(
        "--min-concept-rows",
        type=int,
        default=0,
        help="Fail if concept rowcount is below this threshold (default: 0, disabled)",
    )
    parser.add_argument(
        "--min-concept-relationship-rows",
        type=int,
        default=0,
        help="Fail if concept_relationship rowcount is below this threshold (default: 0, disabled)",
    )
    return parser.parse_args()


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def expected_columns_for(table: str) -> list[str]:
    schema_dir = _repo_root() / "vocabulary_refresh" / "omop_schemas_vocab_bq"
    schema_path = schema_dir / f"{table}.json"
    if not schema_path.exists():
        return []
    cols = json.loads(schema_path.read_text(encoding="utf-8"))
    return [c["name"] for c in cols]


def actual_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()
    return [r[0] for r in rows]


def main() -> int:
    args = parse_args()
    db_path = Path(args.database)
    if not db_path.exists():
        print(f"Missing vocab DuckDB: {db_path}", file=sys.stderr)
        return 2

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        missing: list[str] = []
        for table in REQUIRED_VOCAB_TABLES:
            ok = (
                con.execute(
                    """
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = ? AND table_name = ?
                    LIMIT 1
                    """,
                    [args.schema, table],
                ).fetchone()
                is not None
            )
            if not ok:
                missing.append(table)

        if missing:
            print(
                f"Vocab DuckDB {db_path} is missing tables in schema '{args.schema}': {', '.join(missing)}",
                file=sys.stderr,
            )
            return 1

        if args.validate_columns:
            bad: list[str] = []
            for table in REQUIRED_VOCAB_TABLES:
                expected = set(expected_columns_for(table))
                if not expected:
                    continue
                actual = set(actual_columns(con, args.schema, table))
                missing_cols = sorted(expected - actual)
                if missing_cols:
                    bad.append(f"{table} missing columns: {', '.join(missing_cols)}")
            if bad:
                print("Vocab column validation failed:", file=sys.stderr)
                for line in bad:
                    print(f"- {line}", file=sys.stderr)
                return 1

        if args.min_concept_rows > 0:
            concept_rows = con.execute(f"SELECT COUNT(*) FROM {args.schema}.concept").fetchone()[0]
            if int(concept_rows) < args.min_concept_rows:
                print(
                    f"Vocab rowcount check failed: {args.schema}.concept has {concept_rows} rows (< {args.min_concept_rows})",
                    file=sys.stderr,
                )
                return 3

        if args.min_concept_relationship_rows > 0:
            rel_rows = con.execute(f"SELECT COUNT(*) FROM {args.schema}.concept_relationship").fetchone()[0]
            if int(rel_rows) < args.min_concept_relationship_rows:
                print(
                    f"Vocab rowcount check failed: {args.schema}.concept_relationship has {rel_rows} rows (< {args.min_concept_relationship_rows})",
                    file=sys.stderr,
                )
                return 3
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
