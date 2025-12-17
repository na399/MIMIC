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


class TestDeterministicLoadRowId(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.runner = load_module(self.repo_root / "scripts" / "duckdb_run_script.py", "duckdb_run_script_det")

    def test_staging_load_row_id_is_deterministic(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "x.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA raw_core")
            con.execute("CREATE SCHEMA omop_cdm")
            con.execute(
                "CREATE TABLE raw_core.patients AS SELECT 1::INTEGER AS subject_id, 2020::INTEGER AS anchor_year, 30::INTEGER AS anchor_age, '2000 - 2020' AS anchor_year_group, 'M' AS gender"
            )
            con.execute(
                "CREATE TABLE raw_core.admissions AS SELECT 10::INTEGER AS hadm_id, 1::INTEGER AS subject_id, TIMESTAMP '2020-01-01 00:00:00' AS admittime, TIMESTAMP '2020-01-02 00:00:00' AS dischtime, NULL::TIMESTAMP AS deathtime, 'EMERGENCY' AS admission_type, 'ER' AS admission_location, 'HOME' AS discharge_location, 'WHITE' AS race, TIMESTAMP '2020-01-01 00:00:00' AS edregtime, 'Private' AS insurance, 'SINGLE' AS marital_status, 'EN' AS language"
            )
            con.execute(
                "CREATE TABLE raw_core.transfers AS SELECT 100::INTEGER AS transfer_id, 10::INTEGER AS hadm_id, 1::INTEGER AS subject_id, 'CARDIOLOGY' AS careunit, TIMESTAMP '2020-01-01 01:00:00' AS intime, TIMESTAMP '2020-01-01 02:00:00' AS outtime, 'transfer' AS eventtype"
            )
            con.close()

            config = {
                "variables": {
                    "@duckdb_path": str(db_path),
                    "@etl_project": Path(str(db_path)).stem,
                    "@etl_dataset": "omop_cdm",
                    "@source_project": Path(str(db_path)).stem,
                    "@core_dataset": "raw_core",
                },
                "duckdb": {"database": "@duckdb_path", "pre_sql": ["etl/duckdb/macros.sql"]},
            }

            st_core = str(self.repo_root / "etl" / "staging" / "st_core.sql")
            self.runner.run_scripts([st_core], config)
            con2 = duckdb.connect(str(db_path), read_only=True)
            first = con2.execute("SELECT load_row_id FROM omop_cdm.src_patients").fetchone()[0]
            con2.close()

            self.runner.run_scripts([st_core], config)
            con3 = duckdb.connect(str(db_path), read_only=True)
            second = con3.execute("SELECT load_row_id FROM omop_cdm.src_patients").fetchone()[0]
            con3.close()

            self.assertEqual(first, second)

