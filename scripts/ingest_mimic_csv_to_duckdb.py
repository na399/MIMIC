"""
Load MIMIC-IV CSV folders into DuckDB raw schemas.

The script is intentionally simple: it iterates over configured source folders,
creates schemas (raw_core/raw_hosp/raw_icu/raw_derived by default) and uses
DuckDB's ``read_csv_auto`` to materialize tables.
"""
import argparse
import json
import tarfile
import tempfile
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import duckdb


def discover_files(folder: Path) -> Iterable[Path]:
    for ext in ("*.csv", "*.csv.gz", "*.csv.zip"):
        yield from sorted(folder.glob(ext))


def _looks_like_tar_archive(path: Path) -> bool:
    name = path.name.lower()
    return name.endswith(".tar.gz") or name.endswith(".tgz") or name.endswith(".tar")


def resolve_folder_path(folder: Path, extract_cache: Dict[Path, Path]) -> Path:
    """
    Resolve a folder argument that may be a directory or a tar archive.

    If given a directory with no CSVs but containing a single tar archive, the
    archive is extracted and the extracted directory is used.
    """
    folder = folder.expanduser()
    if folder.is_file() and _looks_like_tar_archive(folder):
        if folder not in extract_cache:
            tmp_dir = Path(tempfile.mkdtemp(prefix="mimic_extract_"))
            with tarfile.open(folder, "r:*") as tf:
                for member in tf.getmembers():
                    member_path = tmp_dir / member.name
                    if not member_path.resolve().is_relative_to(tmp_dir.resolve()):
                        raise ValueError(f"Unsafe path in tar archive: {member.name}")
                tf.extractall(tmp_dir)
            extract_cache[folder] = tmp_dir
        return extract_cache[folder]

    if folder.is_dir():
        csvs = list(discover_files(folder))
        if csvs:
            return folder
        archives = sorted([p for p in folder.iterdir() if p.is_file() and _looks_like_tar_archive(p)])
        if len(archives) == 1:
            return resolve_folder_path(archives[0], extract_cache)
        return folder

    return folder


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


def list_relations(con: duckdb.DuckDBPyConnection, schema: str) -> List[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = ?
        ORDER BY table_name
        """,
        [schema],
    ).fetchall()
    return [r[0] for r in rows]


def table_exists_as_view(con: duckdb.DuckDBPyConnection, schema: str, name: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ? AND table_type = 'VIEW'
        LIMIT 1
        """,
        [schema, name],
    ).fetchone()
    return row is not None


def get_table_columns(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> Dict[str, str]:
    rows = con.execute(f"PRAGMA table_info('{schema}.{table}')").fetchall()
    # (cid, name, type, notnull, dflt_value, pk)
    return {r[1]: r[2] for r in rows}


def _columns_arg(columns: Dict[str, str]) -> str:
    items = ", ".join([f"'{k}': '{v}'" for k, v in columns.items()])
    return "{" + items + "}"


def drop_schema_cascade(con: duckdb.DuckDBPyConnection, schema: str) -> None:
    con.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


CORE_REQUIRED = ("patients", "admissions", "transfers")
HOSP_REQUIRED = (
    "diagnoses_icd",
    "procedures_icd",
    "labevents",
    "d_labitems",
    "prescriptions",
    "drgcodes",
    "hcpcsevents",
    "microbiologyevents",
    "services",
)
ICU_REQUIRED = ("chartevents", "d_items", "datetimeevents", "outputevents", "procedureevents")


def validate_manifest(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    required_tables: Sequence[str],
    *,
    mode: str,
    label: str,
) -> None:
    if mode == "off":
        return
    present = set(list_relations(con, schema))
    missing = [t for t in required_tables if t not in present]
    if not missing:
        return
    msg = f"Manifest check failed for {label} ({schema}): missing {', '.join(missing)}"
    if mode == "warn":
        print("WARNING:", msg)
        return
    raise RuntimeError(msg)

def load_folder(
    con: duckdb.DuckDBPyConnection,
    folder: Path,
    schema: str,
    lowercase: bool = True,
    sample_size: int = -1,
    mode: str = "auto",
    on_exists: str = "replace",
    all_varchar: bool = False,
    type_overrides: Dict[str, Dict[str, str]] | None = None,
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
        present = set(list_relations(con, schema))
        exists = table_name in present
        if exists:
            if on_exists == "skip":
                print(f"Skipping existing {schema}.{table_name}")
                continue
            if on_exists == "fail":
                raise RuntimeError(f"Table already exists: {schema}.{table_name}")
            if on_exists == "append" and table_exists_as_view(con, schema, table_name):
                raise RuntimeError(f"Cannot append into a view: {schema}.{table_name}")
            if on_exists == "replace":
                drop_relation(con, schema, table_name)
        overrides = (type_overrides or {}).get(table_name) or {}
        columns_clause = f", COLUMNS={_columns_arg(overrides)}" if overrides else ""
        csv_expr = (
            f"read_csv_auto({sql_quote_string(str(file_path))}, "
            f"SAMPLE_SIZE={sample_size}, ALL_VARCHAR={'TRUE' if all_varchar else 'FALSE'}, "
            f"HEADER=TRUE, DELIM=',', QUOTE='\"', ESCAPE='\"'{columns_clause})"
        )

        if on_exists == "append" and exists:
            if mode == "copy":
                con.execute(
                    f"""
                    COPY {schema}.{table_name}
                    FROM {sql_quote_string(str(file_path))}
                    (HEADER, DELIM ',', QUOTE '\"', ESCAPE '\"')
                    """
                )
            else:
                columns = get_table_columns(con, schema, table_name)
                csv_expr_typed = (
                    f"read_csv_auto({sql_quote_string(str(file_path))}, "
                    f"SAMPLE_SIZE={sample_size}, ALL_VARCHAR=FALSE, HEADER=TRUE, DELIM=',', QUOTE='\"', ESCAPE='\"', "
                    f"COLUMNS={_columns_arg(columns)})"
                )
                con.execute(f"INSERT INTO {schema}.{table_name} SELECT * FROM {csv_expr_typed}")
            continue

        if mode == "copy":
            con.execute(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM {csv_expr} LIMIT 0")
            con.execute(
                f"""
                COPY {schema}.{table_name}
                FROM {sql_quote_string(str(file_path))}
                (HEADER, DELIM ',', QUOTE '\"', ESCAPE '\"')
                """
            )
        else:
            con.execute(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM {csv_expr}")

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
    parser.add_argument(
        "--mode",
        choices=("auto", "copy"),
        default="auto",
        help="Ingest mode: 'auto' uses read_csv_auto CTAS; 'copy' infers schema then COPY FROM (often faster)",
    )
    parser.add_argument(
        "--on-exists",
        choices=("replace", "append", "skip", "fail"),
        default="replace",
        help="Behavior when a raw table already exists (default: replace)",
    )
    parser.add_argument(
        "--drop-raw",
        action="store_true",
        help="Drop raw schemas before ingest (raw_core/raw_hosp/raw_icu/raw_derived/raw_waveform)",
    )
    parser.add_argument(
        "--manifest",
        choices=("off", "warn", "fail"),
        default="off",
        help="Manifest check for required raw tables per dataset (default: off)",
    )
    parser.add_argument(
        "--all-varchar",
        action="store_true",
        help="Force all ingested columns to VARCHAR (reduces type inference surprises)",
    )
    parser.add_argument(
        "--type-overrides",
        type=Path,
        default=None,
        help="JSON file mapping table -> {column: type} to override CSV type inference",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    con = duckdb.connect(args.database)
    extract_cache: Dict[Path, Path] = {}
    type_overrides: Dict[str, Dict[str, str]] = {}
    if args.type_overrides:
        overrides_path = args.type_overrides
        type_overrides = json.loads(overrides_path.read_text(encoding="utf-8"))

    if args.drop_raw:
        for schema in (
            args.raw_core_schema,
            args.raw_hosp_schema,
            args.raw_icu_schema,
            args.raw_derived_schema,
            args.raw_waveform_schema,
        ):
            drop_schema_cascade(con, schema)

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
        resolved = resolve_folder_path(folder, extract_cache)
        norm_folder = resolved.expanduser().resolve()
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
            mode=args.mode,
            on_exists=args.on_exists,
            all_varchar=args.all_varchar,
            type_overrides=type_overrides,
        )
        for alias_schema in schemas[1:]:
            create_schema_views(con, canonical_schema, alias_schema)

    if args.core_dir:
        validate_manifest(con, args.raw_core_schema, CORE_REQUIRED, mode=args.manifest, label="core")
    if args.hosp_dir:
        validate_manifest(con, args.raw_hosp_schema, HOSP_REQUIRED, mode=args.manifest, label="hosp")
    if args.icu_dir:
        validate_manifest(con, args.raw_icu_schema, ICU_REQUIRED, mode=args.manifest, label="icu")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
