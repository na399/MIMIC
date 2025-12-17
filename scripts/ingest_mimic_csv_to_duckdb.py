"""
Load MIMIC-IV CSV folders into DuckDB raw schemas.

The script is intentionally simple: it iterates over configured source folders,
creates schemas (raw_hosp/raw_icu/raw_derived by default) and uses
DuckDB's ``read_csv_auto`` to materialize tables.
"""
import argparse
import csv
import json
import re
import tarfile
import tempfile
import zipfile
from collections import OrderedDict
from collections.abc import Mapping
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import duckdb


DEFAULT_MIMIC_DDL_PATH = (
    Path(__file__).resolve().parents[1] / "etl" / "mimic_ddl" / "mimiciv_hosp_icu_create_tables.sql"
)


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


HOSP_REQUIRED = (
    "patients",
    "admissions",
    "transfers",
    "diagnoses_icd",
    "procedures_icd",
    "labevents",
    "d_labitems",
    "prescriptions",
    "drgcodes",
    "hcpcsevents",
    "microbiologyevents",
    "services",
    "pharmacy",
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


def sql_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _split_sql_columns(column_block: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    for ch in column_block:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(0, depth - 1)
        if ch == "," and depth == 0:
            part = "".join(buf).strip()
            if part:
                parts.append(part)
            buf = []
            continue
        buf.append(ch)
    last = "".join(buf).strip()
    if last:
        parts.append(last)
    return parts


def _normalize_mimic_type(type_sql: str) -> str:
    t = type_sql.strip()
    t = re.sub(r"TIMESTAMP\(\s*\d+\s*\)", "TIMESTAMP", t, flags=re.IGNORECASE)
    t = re.sub(r"DOUBLE\s+PRECISION", "DOUBLE", t, flags=re.IGNORECASE)
    return t


def parse_mimic_create_tables(ddl_text: str) -> Dict[str, Dict[str, "OrderedDict[str, str]"]]:
    """
    Parse CREATE TABLE blocks from the official MIMIC-IV PostgreSQL DDL.

    Returns:
        {schema_name: {table_name: OrderedDict(column_name -> duckdb_type_sql)}}

    Notes:
        - Constraints (NOT NULL, etc.) are intentionally dropped to make CSV loads
          resilient to empty strings and partial fixtures.
        - TIMESTAMP(n) is normalized to TIMESTAMP for DuckDB compatibility.
    """
    # Strip SQL line comments to simplify parsing.
    stripped = re.sub(r"(?m)^[ \t]*--.*?$", "", ddl_text)

    tables: Dict[str, Dict[str, "OrderedDict[str, str]"]] = {}
    pattern = re.compile(
        r"CREATE\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*\((.*?)\)\s*;",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in pattern.finditer(stripped):
        schema = match.group(1).lower()
        table = match.group(2).lower()
        body = match.group(3)
        columns = OrderedDict()
        for col_def in _split_sql_columns(body):
            col_def = col_def.strip()
            if not col_def:
                continue
            # Ignore table constraints if present.
            upper = col_def.upper()
            if upper.startswith("CONSTRAINT") or upper.startswith("PRIMARY KEY") or upper.startswith("FOREIGN KEY"):
                continue
            parts = col_def.split()
            if len(parts) < 2:
                continue
            col_name = parts[0].strip('"').lower()
            rest = " ".join(parts[1:])
            # Drop common constraints from column definitions.
            rest = re.sub(r"\bNOT\s+NULL\b", "", rest, flags=re.IGNORECASE).strip()
            rest = re.sub(r"\bNULL\b", "", rest, flags=re.IGNORECASE).strip()
            col_type = _normalize_mimic_type(rest)
            columns[col_name] = col_type
        if columns:
            tables.setdefault(schema, {})[table] = columns
    return tables


def read_csv_header(path: Path) -> List[str]:
    if path.suffix.lower() == ".gz":
        import gzip

        with gzip.open(path, "rt", newline="") as f:
            header_line = f.readline()
    elif path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as zf:
            members = [m for m in zf.namelist() if not m.endswith("/")]
            if not members:
                return []
            with zf.open(members[0]) as fp:
                header_line = fp.readline().decode("utf-8", errors="replace")
    else:
        with path.open("r", newline="") as f:
            header_line = f.readline()
    if not header_line:
        return []
    row = next(csv.reader([header_line]))
    return [c.strip() for c in row if c is not None]


def create_table_from_columns(
    con: duckdb.DuckDBPyConnection,
    schema: str,
    table: str,
    columns: Mapping[str, str],
) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_ident(schema)}")
    col_defs = ", ".join([f"{sql_ident(name)} {dtype}" for name, dtype in columns.items()])
    con.execute(f"CREATE TABLE {sql_ident(schema)}.{sql_ident(table)} ({col_defs})")


def _select_expr_for_cast(col: str, dtype: str) -> str:
    dtype_upper = dtype.strip().upper()
    if "CHAR" in dtype_upper or "TEXT" in dtype_upper or "VARCHAR" in dtype_upper:
        return f"{sql_ident(col)} AS {sql_ident(col)}"
    if dtype_upper in ("DATE", "TIMESTAMP"):
        return f"TRY_CAST(NULLIF({sql_ident(col)}, '') AS {dtype}) AS {sql_ident(col)}"
    return f"TRY_CAST(NULLIF({sql_ident(col)}, '') AS {dtype}) AS {sql_ident(col)}"


def load_folder_using_ddl(
    con: duckdb.DuckDBPyConnection,
    folder: Path,
    schema: str,
    *,
    ddl_schema: str,
    ddl_tables: Mapping[str, "OrderedDict[str, str]"],
    lowercase: bool,
    on_exists: str,
    mode: str,
    row_limit: int | None,
    include_tables: set[str] | None,
    all_varchar: bool,
    type_overrides: Dict[str, Dict[str, str]] | None,
) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_ident(schema)}")
    for file_path in discover_files(folder):
        table_name = file_path.stem
        if table_name.endswith(".csv"):
            table_name = table_name[:-4]
        if lowercase:
            table_name = table_name.lower()
        if include_tables is not None and table_name not in include_tables:
            continue

        table_def = ddl_tables.get(table_name)
        if not table_def:
            print(f"WARNING: No DDL for {ddl_schema}.{table_name}; skipping {file_path}")
            continue

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
        final_cols: "OrderedDict[str, str]" = OrderedDict()
        for col, dtype in table_def.items():
            final_dtype = "VARCHAR" if all_varchar else _normalize_mimic_type(overrides.get(col, dtype))
            final_cols[col] = final_dtype

        if not exists or on_exists == "replace":
            create_table_from_columns(con, schema, table_name, final_cols)

        header_cols = read_csv_header(file_path)
        if lowercase:
            header_cols = [c.lower() for c in header_cols]
        table_cols_order = list(final_cols.keys())

        extra_cols = [c for c in header_cols if c not in final_cols]
        if extra_cols:
            print(f"WARNING: {schema}.{table_name} ignoring extra CSV columns: {', '.join(extra_cols)}")

        load_cols = [c for c in header_cols if c in final_cols]
        if not load_cols:
            print(f"WARNING: {file_path} has no columns matching {schema}.{table_name}; skipping")
            continue

        print(f"Loading {file_path} -> {schema}.{table_name}")

        # COPY is fastest but relies on exact column order matching.
        if mode == "copy" and row_limit is None and header_cols == table_cols_order:
            con.execute(
                f"""
                COPY {sql_ident(schema)}.{sql_ident(table_name)}
                FROM {sql_quote_string(str(file_path))}
                (HEADER, DELIM ',', QUOTE '\"', ESCAPE '\"')
                """
            )
            continue

        normalize_names = "TRUE" if lowercase else "FALSE"
        # Provide an explicit column mapping for all header columns so DuckDB
        # does not rename reserved identifiers (e.g., "language" -> "_language")
        # during CSV parsing.
        csv_columns = OrderedDict((c, "VARCHAR") for c in header_cols)
        csv_rel = (
            f"read_csv_auto({sql_quote_string(str(file_path))}, "
            f"HEADER=TRUE, DELIM=',', QUOTE='\"', ESCAPE='\"', "
            f"NORMALIZE_NAMES={normalize_names}, COLUMNS={_columns_arg(csv_columns)})"
        )
        select_list = ", ".join([_select_expr_for_cast(c, final_cols[c]) for c in load_cols])
        insert_cols = ", ".join([sql_ident(c) for c in load_cols])
        limit_clause = f" LIMIT {int(row_limit)}" if row_limit is not None and int(row_limit) > 0 else ""
        con.execute(
            f"""
            INSERT INTO {sql_ident(schema)}.{sql_ident(table_name)} ({insert_cols})
            SELECT {select_list}
            FROM {csv_rel}
            {limit_clause}
            """
        )


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
    row_limit: int | None = None,
    include_tables: set[str] | None = None,
) -> None:
    if not folder.exists():
        raise FileNotFoundError(folder)
    if row_limit is not None and int(row_limit) > 0 and mode == "copy":
        raise ValueError("--row-limit is not supported with --mode copy (COPY FROM loads full files).")
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    for file_path in discover_files(folder):
        table_name = file_path.stem
        if table_name.endswith(".csv"):
            table_name = table_name[:-4]
        if lowercase:
            table_name = table_name.lower()
        if include_tables is not None and table_name not in include_tables:
            continue
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
        limit_clause = f" LIMIT {int(row_limit)}" if row_limit is not None and int(row_limit) > 0 else ""

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
                con.execute(f"INSERT INTO {schema}.{table_name} SELECT * FROM {csv_expr_typed}{limit_clause}")
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
            con.execute(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM {csv_expr}{limit_clause}")

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
    parser.add_argument("--hosp-dir", type=Path, help="Path to MIMIC-IV hosp CSV folder")
    parser.add_argument("--icu-dir", type=Path, help="Path to MIMIC-IV icu CSV folder")
    parser.add_argument("--derived-dir", type=Path, help="Path to MIMIC-IV derived CSV folder")
    parser.add_argument("--waveform-dir", type=Path, help="Path to waveform CSV folder")
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
        help="Drop raw schemas before ingest (raw_hosp/raw_icu/raw_derived/raw_waveform)",
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
    parser.add_argument(
        "--include-tables",
        default="",
        help="Comma-separated list of table names to ingest (default: all tables in the folder)",
    )
    parser.add_argument(
        "--row-limit",
        type=int,
        default=0,
        help="Optional per-table row limit for smoke tests (0 disables; requires --mode auto)",
    )
    parser.add_argument(
        "--ddl-path",
        type=Path,
        default=DEFAULT_MIMIC_DDL_PATH,
        help="Path to official MIMIC-IV CREATE TABLE DDL used for schema (default: repo DDL)",
    )
    parser.add_argument(
        "--infer-schema",
        action="store_true",
        help="Fallback to DuckDB CSV type inference instead of using the official DDL",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.row_limit and int(args.row_limit) > 0 and args.mode == "copy":
        raise ValueError("--row-limit is not supported with --mode copy (COPY FROM loads full files).")
    con = duckdb.connect(args.database)
    extract_cache: Dict[Path, Path] = {}
    type_overrides: Dict[str, Dict[str, str]] = {}
    if args.type_overrides:
        overrides_path = args.type_overrides
        type_overrides = json.loads(overrides_path.read_text(encoding="utf-8"))

    include_tables: set[str] | None = None
    if args.include_tables and args.include_tables.strip():
        names = [n.strip() for n in args.include_tables.split(",") if n.strip()]
        include_tables = set([n if args.keep_case else n.lower() for n in names])

    if args.drop_raw:
        for schema in (
            args.raw_hosp_schema,
            args.raw_icu_schema,
            args.raw_derived_schema,
            args.raw_waveform_schema,
        ):
            drop_schema_cascade(con, schema)

    # Use an ordered list rather than a dict: some datasets (e.g., PhysioNet
    # mimic-iv-demo) colocate multiple logical datasets under the same folder,
    # and users may intentionally point multiple logical inputs at the same
    # directory. A dict would silently drop earlier entries.
    mapping: List[Tuple[Path, str, str]] = []
    if args.hosp_dir:
        mapping.append((args.hosp_dir, args.raw_hosp_schema, "mimiciv_hosp"))
    if args.icu_dir:
        mapping.append((args.icu_dir, args.raw_icu_schema, "mimiciv_icu"))
    if args.derived_dir:
        mapping.append((args.derived_dir, args.raw_derived_schema, "mimiciv_derived"))
    if args.waveform_dir:
        mapping.append((args.waveform_dir, args.raw_waveform_schema, "waveform"))

    # If users point multiple logical datasets at the same folder (common for
    # mimic-iv-demo), load the folder once and create schema views for the
    # duplicate schemas to avoid duplication.
    folder_to_schemas: "OrderedDict[Tuple[Path, str], List[str]]" = OrderedDict()
    for folder, schema, ddl_schema in mapping:
        resolved = resolve_folder_path(folder, extract_cache)
        norm_folder = resolved.expanduser().resolve()
        key = (norm_folder, ddl_schema)
        folder_to_schemas.setdefault(key, [])
        if schema not in folder_to_schemas[key]:
            folder_to_schemas[key].append(schema)

    ddl_defs: Dict[str, Dict[str, "OrderedDict[str, str]"]] = {}
    if not args.infer_schema and args.ddl_path:
        ddl_path = args.ddl_path.expanduser()
        if not ddl_path.exists():
            raise FileNotFoundError(f"DDL file not found: {ddl_path}")
        ddl_defs = parse_mimic_create_tables(ddl_path.read_text(encoding="utf-8"))

    for (folder, ddl_schema), schemas in folder_to_schemas.items():
        canonical_schema = schemas[0]
        if args.infer_schema or ddl_schema == "waveform":
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
                row_limit=args.row_limit if args.row_limit and args.row_limit > 0 else None,
                include_tables=include_tables,
            )
        else:
            if ddl_schema not in ddl_defs:
                if ddl_schema in ("mimiciv_hosp", "mimiciv_icu"):
                    raise RuntimeError(f"DDL schema '{ddl_schema}' not found in {args.ddl_path}")
                print(f"WARNING: DDL schema '{ddl_schema}' not found in {args.ddl_path}; falling back to inference")
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
                    row_limit=args.row_limit if args.row_limit and args.row_limit > 0 else None,
                    include_tables=include_tables,
                )
                for alias_schema in schemas[1:]:
                    create_schema_views(con, canonical_schema, alias_schema)
                continue
            load_folder_using_ddl(
                con,
                folder,
                canonical_schema,
                ddl_schema=ddl_schema,
                ddl_tables=ddl_defs[ddl_schema],
                lowercase=not args.keep_case,
                on_exists=args.on_exists,
                mode=args.mode,
                row_limit=args.row_limit if args.row_limit and args.row_limit > 0 else None,
                include_tables=include_tables,
                all_varchar=args.all_varchar,
                type_overrides=type_overrides,
            )
        for alias_schema in schemas[1:]:
            create_schema_views(con, canonical_schema, alias_schema)

    if args.hosp_dir:
        validate_manifest(con, args.raw_hosp_schema, HOSP_REQUIRED, mode=args.manifest, label="hosp")
    if args.icu_dir:
        validate_manifest(con, args.raw_icu_schema, ICU_REQUIRED, mode=args.manifest, label="icu")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
