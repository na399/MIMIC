"""
Drop intermediate schemas after a successful ETL run and optionally VACUUM the DuckDB file.

This is intended to run after audits: if audits fail, the workflow should stop
before cleanup so intermediate tables remain available for debugging.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def _truthy(value: str) -> bool:
    return str(value).strip().lower() in ("1", "true", "yes", "y", "on")


def sql_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drop intermediate schemas and optionally VACUUM the DuckDB file")
    parser.add_argument("--database", required=True, help="DuckDB database file to clean up")
    parser.add_argument("--enable", default="1", help="Set to 1 to run cleanup; otherwise no-op")
    parser.add_argument("--vacuum", default="1", help="Set to 1 to VACUUM after dropping schemas")

    parser.add_argument("--etl-schema", default="omop_cdm", help="Intermediate ETL schema to drop (default: omop_cdm)")
    parser.add_argument("--raw-hosp-schema", default="raw_hosp", help="Raw hosp schema to drop (default: raw_hosp)")
    parser.add_argument("--raw-icu-schema", default="raw_icu", help="Raw icu schema to drop (default: raw_icu)")
    parser.add_argument("--raw-derived-schema", default="raw_derived", help="Raw derived schema to drop (default: raw_derived)")
    parser.add_argument("--raw-waveform-schema", default="raw_waveform", help="Raw waveform schema to drop (default: raw_waveform)")
    parser.add_argument("--ingest-schema", default="ingest", help="Ingest metadata schema to drop (default: ingest)")

    parser.add_argument("--output-schema", default="main", help="Schema holding final OMOP tables (default: main)")
    parser.add_argument("--audit-schema", default="audit", help="Audit schema to preserve (default: audit)")
    parser.add_argument(
        "--drop-legacy-omop-schema",
        default="1",
        help="Drop the legacy 'omop' schema from older runs if present (default: 1)",
    )
    parser.add_argument(
        "--legacy-omop-schema",
        default="omop",
        help="Legacy published schema name to drop when enabled (default: omop)",
    )
    return parser.parse_args()


def drop_schema(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    if not schema:
        return
    con.execute(f"DROP SCHEMA IF EXISTS {sql_ident(schema)} CASCADE")


def main() -> int:
    args = parse_args()
    if not _truthy(args.enable):
        print("Cleanup disabled; skipping.")
        return 0

    db_path = Path(args.database).expanduser()
    if not db_path.exists():
        raise FileNotFoundError(db_path)

    preserve = {args.output_schema, args.audit_schema, "main", "information_schema", "pg_catalog"}

    schemas_to_drop = [
        args.etl_schema,
        args.raw_hosp_schema,
        args.raw_icu_schema,
        args.raw_derived_schema,
        args.raw_waveform_schema,
        args.ingest_schema,
    ]
    if _truthy(args.drop_legacy_omop_schema):
        schemas_to_drop.append(args.legacy_omop_schema)

    con = duckdb.connect(str(db_path))
    try:
        for schema in schemas_to_drop:
            if not schema:
                continue
            if schema in preserve:
                continue
            drop_schema(con, schema)
            print(f"Dropped schema: {schema}")

        if _truthy(args.vacuum):
            print("Running VACUUM...")
            con.execute("VACUUM")
            print("VACUUM complete.")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())

