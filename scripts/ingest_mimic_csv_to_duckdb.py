"""
Load MIMIC-IV CSV folders into DuckDB raw schemas.

The script is intentionally simple: it iterates over configured source folders,
creates schemas (raw_core/raw_hosp/raw_icu/raw_derived by default) and uses
DuckDB's ``read_csv_auto`` to materialize tables.
"""
import argparse
from collections import OrderedDict
from pathlib import Path
from typing import Iterable, List, Tuple

import duckdb


def discover_files(folder: Path) -> Iterable[Path]:
    for ext in ("*.csv", "*.csv.gz", "*.csv.zip"):
        yield from sorted(folder.glob(ext))


def drop_relation(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> None:
    row = con.execute(
        """
        SELECT table_type
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema, name],
    ).fetchone()
    if not row:
        return
    table_type = str(row[0] or "").upper()
    if table_type == "VIEW":
        con.execute(f"DROP VIEW IF EXISTS {schema}.{name}")
    else:
        con.execute(f"DROP TABLE IF EXISTS {schema}.{name}")


def sql_quote_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def load_folder(
    con: duckdb.DuckDBPyConnection,
    folder: Path,
    schema: str,
    lowercase: bool = True,
    sample_size: int = -1,
) -> None:
    if not folder.exists():
        raise FileNotFoundError(folder)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for file_path in discover_files(folder):
        table_name = file_path.stem
        if table_name.endswith(".csv"):
            table_name = table_name[:-4]
        if lowercase:
            table_name = table_name.lower()
        print(f"Loading {file_path} -> {schema}.{table_name}")
        # Prior runs may have materialized a table or a view (e.g., schema
        # deduplication aliases). Drop whichever exists so we can write a table.
        drop_relation(con, schema, table_name)
        query = (
            f"CREATE OR REPLACE TABLE {schema}.{table_name} AS "
            f"SELECT * FROM read_csv_auto({sql_quote_string(str(file_path))}, "
            f"SAMPLE_SIZE={sample_size}, ALL_VARCHAR=FALSE, HEADER=TRUE, DELIM=',', QUOTE='\"', ESCAPE='\"')"
        )
        con.execute(query)

def list_tables(con: duckdb.DuckDBPyConnection, schema: str) -> List[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ? AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        [schema],
    ).fetchall()
    return [r[0] for r in rows]


def create_schema_views(con: duckdb.DuckDBPyConnection, source_schema: str, target_schema: str) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    # Clear existing relations in the target schema so we don't leave behind
    # stale tables/views from previous runs.
    existing = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
        """,
        [target_schema],
    ).fetchall()
    for (name,) in existing:
        drop_relation(con, target_schema, name)
    for table_name in list_tables(con, source_schema):
        drop_relation(con, target_schema, table_name)
        con.execute(
            f"CREATE OR REPLACE VIEW {target_schema}.{table_name} AS SELECT * FROM {source_schema}.{table_name}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest MIMIC-IV CSV files into DuckDB")
    parser.add_argument("--database", required=True, help="DuckDB file to create or update")
    parser.add_argument("--core-dir", type=Path, help="Path to MIMIC-IV core CSV folder")
    parser.add_argument("--hosp-dir", type=Path, help="Path to MIMIC-IV hosp CSV folder")
    parser.add_argument("--icu-dir", type=Path, help="Path to MIMIC-IV icu CSV folder")
    parser.add_argument("--derived-dir", type=Path, help="Path to MIMIC-IV derived CSV folder")
    parser.add_argument("--waveform-dir", type=Path, help="Path to waveform CSV folder")
    parser.add_argument("--raw-core-schema", default="raw_core")
    parser.add_argument("--raw-hosp-schema", default="raw_hosp")
    parser.add_argument("--raw-icu-schema", default="raw_icu")
    parser.add_argument("--raw-derived-schema", default="raw_derived")
    parser.add_argument("--raw-waveform-schema", default="raw_waveform")
    parser.add_argument("--keep-case", action="store_true", help="Preserve source filename case")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=-1,
        help="DuckDB CSV type inference sample size (-1 scans all rows; faster values may reduce accuracy)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    con = duckdb.connect(args.database)

    # Use an ordered list rather than a dict: some datasets (e.g., PhysioNet
    # mimic-iv-demo) colocate "core" CSVs under the hosp folder, and users may
    # intentionally point multiple logical inputs at the same directory. A dict
    # would silently drop earlier entries.
    mapping: List[Tuple[Path, str]] = []
    if args.core_dir:
        mapping.append((args.core_dir, args.raw_core_schema))
    if args.hosp_dir:
        mapping.append((args.hosp_dir, args.raw_hosp_schema))
    if args.icu_dir:
        mapping.append((args.icu_dir, args.raw_icu_schema))
    if args.derived_dir:
        mapping.append((args.derived_dir, args.raw_derived_schema))
    if args.waveform_dir:
        mapping.append((args.waveform_dir, args.raw_waveform_schema))

    # If users point multiple logical datasets at the same folder (common for
    # mimic-iv-demo, where core tables live under hosp), load the folder once
    # and create schema views for the duplicate schemas to avoid duplication.
    folder_to_schemas: "OrderedDict[Path, List[str]]" = OrderedDict()
    for folder, schema in mapping:
        norm_folder = folder.expanduser().resolve()
        folder_to_schemas.setdefault(norm_folder, [])
        if schema not in folder_to_schemas[norm_folder]:
            folder_to_schemas[norm_folder].append(schema)

    for folder, schemas in folder_to_schemas.items():
        canonical_schema = schemas[0]
        load_folder(
            con,
            folder,
            canonical_schema,
            lowercase=not args.keep_case,
            sample_size=args.sample_size,
        )
        for alias_schema in schemas[1:]:
            create_schema_views(con, canonical_schema, alias_schema)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
