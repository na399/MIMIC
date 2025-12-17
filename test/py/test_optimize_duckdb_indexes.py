from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import duckdb


class TestOptimizeDuckDBIndexes(unittest.TestCase):
    def test_optimize_creates_indexes_when_enabled(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        script = repo_root / "scripts" / "optimize_duckdb_indexes.py"

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "out.duckdb"
            con = duckdb.connect(str(db_path))
            try:
                con.execute("CREATE SCHEMA omop_cdm")
                con.execute("CREATE TABLE omop_cdm.src_patients(subject_id INTEGER)")
                con.execute("CREATE TABLE omop_cdm.src_admissions(hadm_id INTEGER, subject_id INTEGER)")
            finally:
                con.close()

            res = subprocess.run(
                [
                    "python3",
                    str(script),
                    "--database",
                    str(db_path),
                    "--schema",
                    "omop_cdm",
                    "--enable",
                    "1",
                ],
                capture_output=True,
                text=True,
                cwd=repo_root,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)

            con2 = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con2.execute(
                    """
                    SELECT index_name
                    FROM duckdb_indexes()
                    WHERE schema_name = 'omop_cdm' AND table_name IN ('src_patients', 'src_admissions')
                    ORDER BY index_name
                    """
                ).fetchall()
                index_names = [r[0] for r in rows]
                self.assertIn("idx_src_patients_subject_id", index_names)
                self.assertIn("idx_src_admissions_hadm_id", index_names)
            finally:
                con2.close()

    def test_optimize_is_noop_when_disabled(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        script = repo_root / "scripts" / "optimize_duckdb_indexes.py"

        with tempfile.TemporaryDirectory() as td:
            db_path = Path(td) / "out.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA omop_cdm")
            con.execute("CREATE TABLE omop_cdm.src_patients(subject_id INTEGER)")
            con.close()

            res = subprocess.run(
                ["python3", str(script), "--database", str(db_path), "--schema", "omop_cdm", "--enable", "0"],
                capture_output=True,
                text=True,
                cwd=repo_root,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con2 = duckdb.connect(str(db_path), read_only=True)
            try:
                count = con2.execute(
                    "SELECT COUNT(*) FROM duckdb_indexes() WHERE schema_name='omop_cdm' AND table_name='src_patients'"
                ).fetchone()[0]
                self.assertEqual(count, 0)
            finally:
                con2.close()

