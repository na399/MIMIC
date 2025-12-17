"""
Optional DuckDB performance helpers for full MIMIC-IV v3.x runs.

This script creates a curated set of DuckDB indexes on frequently-joined
columns in intermediate ETL tables (typically in the `@etl_dataset` schema).

Notes:
  - Index build time can be significant on very large tables.
  - DuckDB often prefers sequential scans; indexes are most useful for
    selective predicates and smaller dimension-like tables.
  - The ETL runs fine without this script; keep it disabled unless profiling
    shows a benefit.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

import duckdb


@dataclass(frozen=True)
class IndexSpec:
    table: str
    name: str
    columns: Sequence[str]
    large: bool = False


INDEX_SPECS: tuple[IndexSpec, ...] = (
    # Core join keys (small/medium)
    IndexSpec("src_patients", "idx_src_patients_subject_id", ("subject_id",)),
    IndexSpec("src_admissions", "idx_src_admissions_hadm_id", ("hadm_id",)),
    IndexSpec("src_admissions", "idx_src_admissions_subject_id", ("subject_id",)),
    IndexSpec("src_transfers", "idx_src_transfers_hadm_id", ("hadm_id",)),
    IndexSpec("src_services", "idx_src_services_hadm_id", ("hadm_id",)),
    IndexSpec("src_diagnoses_icd", "idx_src_diagnoses_icd_hadm_id", ("hadm_id",)),
    IndexSpec("src_procedures_icd", "idx_src_procedures_icd_hadm_id", ("hadm_id",)),
    IndexSpec("src_hcpcsevents", "idx_src_hcpcsevents_hadm_id", ("hadm_id",)),
    IndexSpec("src_prescriptions", "idx_src_prescriptions_hadm_id", ("hadm_id",)),
    IndexSpec("src_microbiologyevents", "idx_src_microbiologyevents_hadm_id", ("hadm_id",)),
    IndexSpec("src_d_labitems", "idx_src_d_labitems_itemid", ("itemid",)),
    IndexSpec("src_d_items", "idx_src_d_items_itemid", ("itemid",)),
    # Large fact-like sources (disabled by default)
    IndexSpec("src_labevents", "idx_src_labevents_itemid", ("itemid",), large=True),
    IndexSpec("src_labevents", "idx_src_labevents_hadm_id", ("hadm_id",), large=True),
    IndexSpec("src_chartevents", "idx_src_chartevents_itemid", ("itemid",), large=True),
    IndexSpec("src_chartevents", "idx_src_chartevents_stay_id", ("stay_id",), large=True),
    IndexSpec("src_outputevents", "idx_src_outputevents_itemid", ("itemid",), large=True),
    IndexSpec("src_outputevents", "idx_src_outputevents_stay_id", ("stay_id",), large=True),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create optional DuckDB indexes for ETL performance")
    parser.add_argument("--database", required=True, help="DuckDB database to optimize")
    parser.add_argument("--schema", default="omop_cdm", help="Schema containing intermediate ETL tables")
    parser.add_argument("--enable", type=int, default=0, help="Set to 1 to create indexes")
    parser.add_argument("--enable-large", type=int, default=0, help="Set to 1 to also index large fact-like tables")
    return parser.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ? AND table_type = 'BASE TABLE'
        LIMIT 1
        """,
        [schema, table],
    ).fetchone()
    return row is not None


def sql_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def iter_enabled_specs(enable_large: bool) -> Iterable[IndexSpec]:
    for spec in INDEX_SPECS:
        if spec.large and not enable_large:
            continue
        yield spec


def create_indexes(db_path: Path, schema: str, *, enable_large: bool) -> int:
    con = duckdb.connect(str(db_path))
    try:
        created = 0
        for spec in iter_enabled_specs(enable_large):
            if not table_exists(con, schema, spec.table):
                continue
            cols = ", ".join([sql_ident(c) for c in spec.columns])
            con.execute(
                f"CREATE INDEX IF NOT EXISTS {sql_ident(spec.name)} "
                f"ON {sql_ident(schema)}.{sql_ident(spec.table)} ({cols})"
            )
            created += 1
        con.execute("ANALYZE")
        return created
    finally:
        con.close()


def main() -> int:
    args = parse_args()
    db_path = Path(os.path.expandvars(args.database)).expanduser()
    if not db_path.exists():
        raise FileNotFoundError(db_path)
    if int(args.enable) != 1:
        print("DuckDB optimize disabled (set --enable 1 to run).")
        return 0
    created = create_indexes(db_path, args.schema, enable_large=int(args.enable_large) == 1)
    print(f"Created/verified {created} index(es) in schema '{args.schema}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
