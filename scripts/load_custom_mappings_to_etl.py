"""
Load custom mapping CSVs into the ETL vocabulary tables (voc_concept, voc_concept_relationship).

The ETL SQL uses custom vocabularies like "mimiciv_meas_lab_loinc" and expects
their concepts to exist in omop_cdm.voc_concept and be linked via "Maps to" in
omop_cdm.voc_concept_relationship.

This script ingests the curated mapping files under custom_mapping_csv/ that
already contain both the custom concept rows and their target concept ids.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import duckdb


@dataclass(frozen=True)
class ConceptRow:
    concept_id: int
    concept_name: str
    domain_id: str
    vocabulary_id: str
    concept_class_id: str
    standard_concept: Optional[str]
    concept_code: str
    valid_start_date: str
    valid_end_date: str
    invalid_reason: Optional[str]


@dataclass(frozen=True)
class ConceptRelRow:
    concept_id_1: int
    concept_id_2: int
    relationship_id: str
    valid_start_date: str
    valid_end_date: str
    invalid_reason: Optional[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load custom mapping concepts into ETL vocab tables")
    parser.add_argument("--database", required=True, help="DuckDB database containing the ETL schemas")
    parser.add_argument("--etl-schema", default="omop_cdm", help="Schema holding voc_* tables (default: omop_cdm)")
    parser.add_argument("--mapping-dir", default="custom_mapping_csv", help="Folder containing gcpt_*.csv files")
    return parser.parse_args()


def _norm(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = s.strip()
    return s2 if s2 != "" else None


def iter_mapping_files(mapping_dir: Path) -> Iterable[Path]:
    yield from sorted(mapping_dir.glob("gcpt_*.csv"))


def read_rows(csv_path: Path) -> tuple[list[ConceptRow], list[ConceptRelRow]]:
    concepts: dict[int, ConceptRow] = {}
    relationships: set[ConceptRelRow] = set()
    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            return [], []
        # Normalize known header typos.
        fieldnames = [f.strip() for f in reader.fieldnames]
        rename = {"reverese_relationship_id": "reverse_relationship_id"}
        reader.fieldnames = [rename.get(name, name) for name in fieldnames]

        for row in reader:
            source_concept_id = _norm(row.get("source_concept_id"))
            if source_concept_id is None:
                continue
            concept_id = int(source_concept_id)

            concept = ConceptRow(
                concept_id=concept_id,
                concept_name=_norm(row.get("concept_name")) or "",
                domain_id=_norm(row.get("source_domain_id")) or "",
                vocabulary_id=_norm(row.get("source_vocabulary_id")) or "",
                concept_class_id=_norm(row.get("source_concept_class_id")) or "",
                standard_concept=_norm(row.get("standard_concept")),
                concept_code=_norm(row.get("concept_code")) or "",
                valid_start_date=_norm(row.get("valid_start_date")) or "1970-01-01",
                valid_end_date=_norm(row.get("valid_end_date")) or "2099-12-31",
                invalid_reason=_norm(row.get("invalid_reason")),
            )
            concepts[concept_id] = concept

            target_concept_id = _norm(row.get("target_concept_id"))
            relationship_id = _norm(row.get("relationship_id"))
            if target_concept_id and relationship_id:
                relationships.add(
                    ConceptRelRow(
                        concept_id_1=concept_id,
                        concept_id_2=int(target_concept_id),
                        relationship_id=relationship_id,
                        valid_start_date=_norm(row.get("relationship_valid_start_date")) or "1970-01-01",
                        valid_end_date=_norm(row.get("relationship_end_date")) or "2099-12-31",
                        invalid_reason=_norm(row.get("invalid_reason_cr")),
                    )
                )
    return list(concepts.values()), list(relationships)


def main() -> int:
    args = parse_args()
    db_path = Path(args.database)
    mapping_dir = Path(args.mapping_dir)
    if not db_path.exists():
        raise FileNotFoundError(db_path)
    if not mapping_dir.exists():
        raise FileNotFoundError(mapping_dir)

    con = duckdb.connect(str(db_path))
    etl_schema = args.etl_schema

    concept_rows: list[ConceptRow] = []
    rel_rows: list[ConceptRelRow] = []
    for csv_path in iter_mapping_files(mapping_dir):
        concepts, rels = read_rows(csv_path)
        concept_rows.extend(concepts)
        rel_rows.extend(rels)

    if not concept_rows and not rel_rows:
        print(f"No custom mapping rows found under {mapping_dir}")
        return 0

    con.executemany(
        f"""
        INSERT INTO {etl_schema}.voc_custom_concept
        (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept,
         concept_code, valid_start_date, valid_end_date, invalid_reason)
        VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?)
        """,
        [
            (
                r.concept_id,
                r.concept_name,
                r.domain_id,
                r.vocabulary_id,
                r.concept_class_id,
                r.standard_concept,
                r.concept_code,
                r.valid_start_date,
                r.valid_end_date,
                r.invalid_reason,
            )
            for r in concept_rows
        ],
    )

    if rel_rows:
        con.executemany(
            f"""
            INSERT INTO {etl_schema}.voc_custom_concept_relationship
            (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
            VALUES (?, ?, ?, CAST(? AS DATE), CAST(? AS DATE), ?)
            """,
            [
                (
                    r.concept_id_1,
                    r.concept_id_2,
                    r.relationship_id,
                    r.valid_start_date,
                    r.valid_end_date,
                    r.invalid_reason,
                )
                for r in rel_rows
            ],
        )

    print(f"Loaded {len(concept_rows)} custom concepts and {len(rel_rows)} concept relationships from {mapping_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
