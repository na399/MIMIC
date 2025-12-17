from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb


def load_module(path: Path, name: str):
    scripts_dir = path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestConditionSeqNumType(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.runner = load_module(self.repo_root / "scripts" / "duckdb_run_script.py", "duckdb_run_script_cond")

    def test_seq_num_maps_to_condition_type_concept(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "x.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA omop_cdm")

            # Minimal inputs for lk_cond_diagnoses.sql
            con.execute(
                """
                CREATE TABLE omop_cdm.src_diagnoses_icd AS
                SELECT * FROM (
                    VALUES
                      (1, 10, 1, 'I10', 10, 'diagnoses_icd', 1::BIGINT, '{}'),
                      (1, 10, 2, 'I10', 10, 'diagnoses_icd', 2::BIGINT, '{}'),
                      (1, 10, 25, 'I10', 10, 'diagnoses_icd', 3::BIGINT, '{}')
                ) AS t(subject_id, hadm_id, seq_num, icd_code, icd_version, load_table_id, load_row_id, trace_id)
                """
            )
            con.execute(
                """
                CREATE TABLE omop_cdm.src_admissions AS
                SELECT * FROM (
                    VALUES
                      (10, 1, TIMESTAMP '2020-01-01 00:00:00', TIMESTAMP '2020-01-02 00:00:00', NULL::TIMESTAMP,
                       'EMERGENCY', 'ER', 'HOME', 'WHITE', TIMESTAMP '2020-01-01 00:00:00', 'Private', 'SINGLE', 'EN',
                       'admissions', 1::BIGINT, '{}')
                ) AS t(hadm_id, subject_id, admittime, dischtime, deathtime, admission_type, admission_location,
                       discharge_location, ethnicity, edregtime, insurance, marital_status, language,
                       load_table_id, load_row_id, trace_id)
                """
            )

            # Stub vocab views used by mapping joins.
            con.execute(
                "CREATE TABLE omop_cdm.voc_concept(concept_id INTEGER, concept_code VARCHAR, vocabulary_id VARCHAR, domain_id VARCHAR, standard_concept VARCHAR, invalid_reason VARCHAR)"
            )
            con.execute(
                "CREATE TABLE omop_cdm.voc_concept_relationship(concept_id_1 INTEGER, concept_id_2 INTEGER, relationship_id VARCHAR)"
            )
            con.close()

            config = {
                "variables": {
                    "@duckdb_path": str(db_path),
                    "@etl_project": Path(str(db_path)).stem,
                    "@etl_dataset": "omop_cdm",
                },
                "duckdb": {"database": "@duckdb_path"},
            }

            self.runner.run_scripts([str(self.repo_root / "etl" / "etl" / "lk_cond_diagnoses.sql")], config)

            con2 = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con2.execute(
                    "SELECT seq_num, type_concept_id FROM omop_cdm.lk_diagnoses_icd_mapped ORDER BY seq_num"
                ).fetchall()
                self.assertEqual(rows[0], (1, 38000183))
                self.assertEqual(rows[1], (2, 38000185))
                # seq_num is capped at 20 in lk_diagnoses_icd_clean
                self.assertEqual(rows[2], (20, 44818713))
            finally:
                con2.close()

