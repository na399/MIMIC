"""Generate small mock MIMIC-IV CSVs and a minimal vocabulary DuckDB."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Iterable, List

import duckdb


DEFAULT_BASE_DIR = Path("data/mock_mimic")
DEFAULT_VOCAB_DB = Path("data/mock_vocab.duckdb")


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    ensure_dir(path.parent)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def build_sources(base_dir: Path) -> None:
    hosp = base_dir / "hosp"
    icu = base_dir / "icu"
    waveform = base_dir / "waveform"
    derived = base_dir / "derived"
    for folder in (hosp, icu, waveform, derived):
        ensure_dir(folder)

    write_csv(
        hosp / "patients.csv",
        [
            {
                "subject_id": 1,
                "anchor_year": 2020,
                "anchor_age": 30,
                "anchor_year_group": "2000 - 2020",
                "gender": "M",
            }
        ],
    )

    write_csv(
        hosp / "admissions.csv",
        [
            {
                "hadm_id": 10,
                "subject_id": 1,
                "admittime": "2020-01-01 00:00:00",
                "dischtime": "2020-01-02 00:00:00",
                "deathtime": "2020-01-03 00:00:00",
                "admission_type": "EMERGENCY",
                "admission_location": "EMERGENCY ROOM",
                "discharge_location": "HOME",
                "race": "WHITE",
                "edregtime": "2020-01-01 00:00:00",
                "insurance": "Private",
                "marital_status": "SINGLE",
                "language": "EN",
            }
        ],
    )

    write_csv(
        hosp / "transfers.csv",
        [
            {
                "transfer_id": 100,
                "hadm_id": 10,
                "subject_id": 1,
                "careunit": "CARDIOLOGY",
                "intime": "2020-01-01 01:00:00",
                "outtime": "2020-01-01 02:00:00",
                "eventtype": "transfer",
                "prev_service": "MED",
                "curr_service": "SURG",
            }
        ],
    )

    write_csv(
        hosp / "diagnoses_icd.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "seq_num": 1,
                "icd_code": "I10",
                "icd_version": 10,
            }
        ],
    )

    write_csv(
        hosp / "services.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "transfertime": "2020-01-01 00:30:00",
                "prev_service": "CMED",
                "curr_service": "SURG",
            }
        ],
    )

    write_csv(
        hosp / "labevents.csv",
        [
            {
                "labevent_id": 1000,
                "subject_id": 1,
                "charttime": "2020-01-01 05:00:00",
                "hadm_id": 10,
                "itemid": 50868,
                "valueuom": "mg/dL",
                "value": "5.0",
                "flag": "",
                "ref_range_lower": 3.0,
                "ref_range_upper": 7.0,
            }
        ],
    )

    write_csv(
        hosp / "d_labitems.csv",
        [
            {
                "itemid": 50868,
                "label": "Calcium",
                "fluid": "Blood",
                "category": "Chemistry",
            }
        ],
    )

    write_csv(
        hosp / "procedures_icd.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "seq_num": 1,
                "icd_code": "0JH60BZ",
                "icd_version": 10,
            }
        ],
    )

    write_csv(
        hosp / "hcpcsevents.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "hcpcs_cd": "99281",
                "seq_num": 1,
                "short_description": "Emergency visit",
                "ticket_id_seq": 1,
            }
        ],
    )

    write_csv(
        hosp / "drgcodes.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "drg_code": "001",
                "drg_type": "MDC",
                "description": "General",
            }
        ],
    )

    write_csv(
        hosp / "prescriptions.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "pharmacy_id": 1,
                "starttime": "2020-01-01 03:00:00",
                "stoptime": "2020-01-05 03:00:00",
                "drug_type": "MAIN",
                "drug": "ASPIRIN",
                "gsn": "123456",
                "ndc": "12345-6789",
                "prod_strength": "81 MG",
                "form_rx": "TABLET",
                "dose_val_rx": "1",
                "dose_unit_rx": "tablet",
                "form_val_disp": "1",
                "form_unit_disp": "tab",
                "doses_per_24_hrs": "1",
                "route": "ORAL",
            }
        ],
    )

    write_csv(
        hosp / "microbiologyevents.csv",
        [
            {
                "microevent_id": 1,
                "hadm_id": 10,
                "subject_id": 1,
                "chartdate": "2020-01-02",
                "charttime": "2020-01-02 01:00:00",
                "spec_itemid": 1,
                "spec_type_desc": "BLOOD",
                "test_itemid": 2,
                "test_name": "Culture",
                "org_itemid": 100,
                "org_name": "E COLI",
                "ab_itemid": 200,
                "ab_name": "AMPICILLIN",
                "dilution_comparison": "=",
                "dilution_value": 1.0,
                "interpretation": "S",
                "is_positive": 1,
            }
        ],
    )

    write_csv(
        hosp / "pharmacy.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "pharmacy_id": 1,
                "medication": "ASPIRIN",
                "route": "ORAL",
                "starttime": "2020-01-01 03:00:00",
                "endtime": "2020-01-05 03:00:00",
                "dose_val_rx": "1",
                "dose_unit_rx": "tablet",
            }
        ],
    )

    write_csv(
        icu / "procedureevents.csv",
        [
            {
                "hadm_id": 10,
                "subject_id": 1,
                "stay_id": 100,
                "itemid": 225441,
                "starttime": "2020-01-01 04:00:00",
                "value": 1,
                "location": "ICU",
            }
        ],
    )

    write_csv(
        icu / "d_items.csv",
        [
            {
                "itemid": 225441,
                "label": "Test item",
                "linksto": "chartevents",
                "abbreviation": "TI",
            }
        ],
    )

    write_csv(
        icu / "datetimeevents.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "stay_id": 100,
                "itemid": 220045,
                "charttime": "2020-01-01 06:00:00",
                "value": "2020-01-01 06:00:00",
                "storetime": "2020-01-01 06:01:00",
            }
        ],
    )

    write_csv(
        icu / "chartevents.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "stay_id": 100,
                "itemid": 220045,
                "charttime": "2020-01-01 07:00:00",
                "valuenum": 1.23,
                "value": "note",
                "valueuom": "unit",
            }
        ],
    )

    write_csv(
        icu / "outputevents.csv",
        [
            {
                "subject_id": 1,
                "hadm_id": 10,
                "stay_id": 100,
                "charttime": "2020-01-01 08:00:00",
                "storetime": "2020-01-01 08:01:00",
                "itemid": 300,
                "value": 5,
                "valueuom": "mL",
            }
        ],
    )

    write_csv(
        waveform / "wf_header.csv",
        [
            {
                "case_id": 1,
                "subject_id": 1,
                "short_reference_id": "CASE1",
                "long_reference_id": "case1",
            }
        ],
    )

    write_csv(
        waveform / "wf_details.csv",
        [
            {
                "case_id": 1,
                "date_time": "2020-01-01 00:00:00",
                "segment_name": "SEG001",
                "src_name": "HR",
                "value": 70,
                "unit_concept_name": "bpm",
            },
            {
                "case_id": 1,
                "date_time": "2020-01-01 00:01:00",
                "segment_name": "SEG002",
                "src_name": "HR",
                "value": 72,
                "unit_concept_name": "bpm",
            },
        ],
    )


def map_type(bq_type: str) -> str:
    return {
        "INT64": "BIGINT",
        "STRING": "VARCHAR",
        "FLOAT64": "DOUBLE",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
    }.get(bq_type.upper(), "VARCHAR")


def create_vocabulary(vocab_db: Path, *, include_clinical_maps: bool) -> None:
    if vocab_db.exists():
        vocab_db.unlink()
    ensure_dir(vocab_db.parent)
    con = duckdb.connect(str(vocab_db))
    # Keep everything in DuckDB's default schema (main) to match full Athena
    # exports, and also expose a legacy "vocab" schema via views for backwards
    # compatibility with older configs.
    con.execute("CREATE SCHEMA IF NOT EXISTS main")
    con.execute("CREATE SCHEMA IF NOT EXISTS vocab")
    repo_root = Path(__file__).resolve().parents[2]
    schema_dir = repo_root / "vocabulary_refresh" / "omop_schemas_vocab_bq"
    vocab_tables = [
        "concept",
        "concept_relationship",
        "vocabulary",
        "domain",
        "concept_class",
        "relationship",
        "concept_synonym",
        "concept_ancestor",
        "drug_strength",
    ]

    for table in vocab_tables:
        schema_path = schema_dir / f"{table}.json"
        columns: Iterable[Dict[str, str]] = json.loads(schema_path.read_text())
        col_defs = [f"{col['name']} {map_type(col['type'])}" for col in columns]
        ddl = f"CREATE OR REPLACE TABLE main.{table} ({', '.join(col_defs)})"
        con.execute(ddl)
        con.execute(f"CREATE OR REPLACE VIEW vocab.{table} AS SELECT * FROM main.{table}")

    con.execute(
        "INSERT INTO main.concept VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            8527,
            "White",
            "Race",
            "Race",
            "Race",
            "S",
            "WHITE",
            "1970-01-01",
            "2099-12-31",
            None,
        ),
    )
    con.execute(
        "INSERT INTO main.concept VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            38003563,
            "Not Hispanic or Latino",
            "Ethnicity",
            "Ethnicity",
            "Ethnicity",
            "S",
            "NOT HISPANIC OR LATINO",
            "1970-01-01",
            "2099-12-31",
            None,
        ),
    )
    con.execute(
        "INSERT INTO main.concept VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            2000000001,
            "MIMIC White",
            "Race",
            "MIMICIV",
            "Race",
            None,
            "WHITE",
            "1970-01-01",
            "2099-12-31",
            None,
        ),
    )
    con.execute(
        "INSERT INTO main.concept_relationship VALUES (?, ?, ?, ?, ?, ?)",
        (
            2000000001,
            8527,
            "Maps to",
            "1970-01-01",
            "2099-12-31",
            None,
        ),
    )
    con.execute(
        "INSERT INTO main.vocabulary VALUES (?, ?, ?, ?, ?)",
        (
            "Race",
            "Race",
            "mock",
            "v1",
            0,
        ),
    )
    con.execute(
        "INSERT INTO main.vocabulary VALUES (?, ?, ?, ?, ?)",
        (
            "Ethnicity",
            "Ethnicity",
            "mock",
            "v1",
            0,
        ),
    )
    con.execute(
        "INSERT INTO main.vocabulary VALUES (?, ?, ?, ?, ?)",
        (
            "MIMICIV",
            "MIMICIV",
            "mock",
            "v1",
            0,
        ),
    )

    if include_clinical_maps:
        # Minimal clinical mappings so the mock ETL validates that mapping logic works.
        concepts: list[tuple] = [
            # Source ICD10CM diagnosis (I10) -> standard condition concept.
            (
                2000001001,
                "Essential (primary) hypertension (ICD10CM I10)",
                "Condition",
                "ICD10CM",
                "ICD10CM",
                None,
                "I10",
                "1970-01-01",
                "2099-12-31",
                None,
            ),
            (
                2000001002,
                "Hypertensive disorder (mock SNOMED standard)",
                "Condition",
                "SNOMED",
                "Clinical Finding",
                "S",
                "38341003",
                "1970-01-01",
                "2099-12-31",
                None,
            ),
            # Source ICD10PCS procedure -> standard procedure concept.
            (
                2000002001,
                "Mock ICD10PCS procedure (0JH60BZ)",
                "Procedure",
                "ICD10PCS",
                "ICD10PCS",
                None,
                "0JH60BZ",
                "1970-01-01",
                "2099-12-31",
                None,
            ),
            (
                2000002002,
                "Mock procedure (standard)",
                "Procedure",
                "SNOMED",
                "Procedure",
                "S",
                "999000",
                "1970-01-01",
                "2099-12-31",
                None,
            ),
            # Source HCPCS procedure -> standard procedure concept.
            (
                2000003001,
                "Mock HCPCS procedure (99281)",
                "Procedure",
                "HCPCS",
                "HCPCS",
                None,
                "99281",
                "1970-01-01",
                "2099-12-31",
                None,
            ),
            (
                2000003002,
                "Mock procedure 2 (standard)",
                "Procedure",
                "SNOMED",
                "Procedure",
                "S",
                "999001",
                "1970-01-01",
                "2099-12-31",
                None,
            ),
            # Source NDC drug -> standard drug concept.
            (
                2000004001,
                "Mock NDC drug (12345-6789)",
                "Drug",
                "NDC",
                "NDC",
                None,
                "12345-6789",
                "1970-01-01",
                "2099-12-31",
                None,
            ),
            (
                2000004002,
                "Mock Aspirin (standard)",
                "Drug",
                "RxNorm",
                "Ingredient",
                "S",
                "1191",
                "1970-01-01",
                "2099-12-31",
                None,
            ),
        ]

        for row in concepts:
            con.execute("INSERT INTO main.concept VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", row)

        relationships: list[tuple] = [
            (2000001001, 2000001002, "Maps to", "1970-01-01", "2099-12-31", None),
            (2000002001, 2000002002, "Maps to", "1970-01-01", "2099-12-31", None),
            (2000003001, 2000003002, "Maps to", "1970-01-01", "2099-12-31", None),
            (2000004001, 2000004002, "Maps to", "1970-01-01", "2099-12-31", None),
        ]
        for row in relationships:
            con.execute("INSERT INTO main.concept_relationship VALUES (?, ?, ?, ?, ?, ?)", row)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate mock MIMIC-IV CSVs and a minimal vocab DuckDB")
    parser.add_argument("--base-dir", type=Path, default=DEFAULT_BASE_DIR, help="Output folder for mock MIMIC CSVs")
    parser.add_argument("--vocab-db", type=Path, default=DEFAULT_VOCAB_DB, help="Output DuckDB path for minimal vocab")
    parser.add_argument(
        "--no-clinical-maps",
        action="store_true",
        help="Do not add any clinical Maps-to rows (produces a vocab that will result in 0% mapping rates).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_sources(args.base_dir)
    create_vocabulary(args.vocab_db, include_clinical_maps=not args.no_clinical_maps)


if __name__ == "__main__":
    main()
