from __future__ import annotations

import importlib.util
import subprocess
import sys
import tarfile
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


class TestIngest(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.ingest = load_module(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py", "ingest_mimic")

    def test_load_folder_drops_existing_table_not_view(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "admissions.csv").write_text("hadm_id,subject_id\n10,1\n", encoding="utf-8")

            db_path = base / "test.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA raw_core")
            con.execute("CREATE TABLE raw_core.admissions AS SELECT 1 AS x")

            self.ingest.load_folder(con, data_dir, "raw_core", lowercase=True, sample_size=-1)

            row = con.execute(
                """
                SELECT table_type
                FROM information_schema.tables
                WHERE table_schema='raw_core' AND table_name='admissions'
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "BASE TABLE")
            self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_core.admissions").fetchone()[0], 1)
            con.close()

    def test_create_schema_views_replaces_existing_target_relations(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n1\n", encoding="utf-8")

            db_path = base / "test.duckdb"
            con = duckdb.connect(str(db_path))
            self.ingest.load_folder(con, data_dir, "raw_core", lowercase=True, sample_size=-1)

            # First create alias schema views
            self.ingest.create_schema_views(con, "raw_core", "raw_hosp")
            table_type = con.execute(
                """
                SELECT table_type
                FROM information_schema.tables
                WHERE table_schema='raw_hosp' AND table_name='patients'
                """
            ).fetchone()[0]
            self.assertEqual(table_type, "VIEW")

            # Simulate a stale table left behind, then re-run and ensure it becomes a view again.
            con.execute("DROP VIEW raw_hosp.patients")
            con.execute("CREATE TABLE raw_hosp.patients AS SELECT 123 AS subject_id")
            self.ingest.create_schema_views(con, "raw_core", "raw_hosp")
            table_type2 = con.execute(
                """
                SELECT table_type
                FROM information_schema.tables
                WHERE table_schema='raw_hosp' AND table_name='patients'
                """
            ).fetchone()[0]
            self.assertEqual(table_type2, "VIEW")
            self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_hosp.patients").fetchone()[0], 1)
            con.close()

    def test_ingest_accepts_tar_gz_folder(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            archive = base / "csv.tar.gz"
            tmp_root = base / "extract_src"
            tmp_root.mkdir(parents=True, exist_ok=True)
            (tmp_root / "admissions.csv").write_text("hadm_id,subject_id\n10,1\n", encoding="utf-8")
            with tarfile.open(archive, "w:gz") as tf:
                tf.add(tmp_root / "admissions.csv", arcname="admissions.csv")

            db_path = base / "out.duckdb"
            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--core-dir",
                    str(archive),
                    "--raw-core-schema",
                    "raw_core",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con = duckdb.connect(str(db_path), read_only=True)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_core.admissions").fetchone()[0], 1)
            con.close()

    def test_ingest_copy_mode_loads_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n1\n", encoding="utf-8")
            db_path = base / "out.duckdb"

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--core-dir",
                    str(data_dir),
                    "--raw-core-schema",
                    "raw_core",
                    "--mode",
                    "copy",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con = duckdb.connect(str(db_path), read_only=True)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_core.patients").fetchone()[0], 1)
            con.close()

    def test_ingest_append_mode_appends_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "core"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n2\n", encoding="utf-8")

            db_path = base / "out.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA raw_core")
            con.execute("CREATE TABLE raw_core.patients(subject_id INTEGER)")
            con.execute("INSERT INTO raw_core.patients VALUES (1)")
            con.close()

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--core-dir",
                    str(data_dir),
                    "--raw-core-schema",
                    "raw_core",
                    "--on-exists",
                    "append",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con2 = duckdb.connect(str(db_path), read_only=True)
            self.assertEqual(con2.execute("SELECT COUNT(*) FROM raw_core.patients").fetchone()[0], 2)
            con2.close()

    def test_ingest_drop_raw_clears_existing_raw_schemas(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "core"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n2\n", encoding="utf-8")

            db_path = base / "out.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA raw_core")
            con.execute("CREATE TABLE raw_core.patients(subject_id INTEGER)")
            con.execute("INSERT INTO raw_core.patients VALUES (1)")
            con.close()

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--core-dir",
                    str(data_dir),
                    "--raw-core-schema",
                    "raw_core",
                    "--drop-raw",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con2 = duckdb.connect(str(db_path), read_only=True)
            # After dropping raw schemas, only the ingested row should remain.
            self.assertEqual(con2.execute("SELECT COUNT(*) FROM raw_core.patients").fetchone()[0], 1)
            self.assertEqual(con2.execute("SELECT MIN(subject_id) FROM raw_core.patients").fetchone()[0], 2)
            con2.close()

    def test_ingest_manifest_fails_when_missing_required_tables(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "core"
            data_dir.mkdir(parents=True, exist_ok=True)
            # Intentionally omit admissions/transfers.
            (data_dir / "patients.csv").write_text("subject_id\n1\n", encoding="utf-8")
            db_path = base / "out.duckdb"
            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--core-dir",
                    str(data_dir),
                    "--raw-core-schema",
                    "raw_core",
                    "--manifest",
                    "fail",
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(res.returncode, 0)

    def test_ingest_all_varchar_forces_string_types(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "core"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n001\n", encoding="utf-8")
            (data_dir / "admissions.csv").write_text("hadm_id,subject_id\n10,001\n", encoding="utf-8")
            (data_dir / "transfers.csv").write_text("transfer_id,hadm_id,subject_id\n1,10,001\n", encoding="utf-8")
            db_path = base / "out.duckdb"

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--core-dir",
                    str(data_dir),
                    "--raw-core-schema",
                    "raw_core",
                    "--all-varchar",
                    "--manifest",
                    "fail",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                dtype = con.execute("SELECT data_type FROM information_schema.columns WHERE table_schema='raw_core' AND table_name='patients' AND column_name='subject_id'").fetchone()[0]
                self.assertIn("VARCHAR", dtype.upper())
                val = con.execute("SELECT subject_id FROM raw_core.patients").fetchone()[0]
                self.assertEqual(val, "001")
            finally:
                con.close()

    def test_ingest_type_overrides_preserve_leading_zeros(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "core"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n001\n", encoding="utf-8")
            (data_dir / "admissions.csv").write_text("hadm_id,subject_id\n10,001\n", encoding="utf-8")
            (data_dir / "transfers.csv").write_text("transfer_id,hadm_id,subject_id\n1,10,001\n", encoding="utf-8")
            overrides = base / "overrides.json"
            overrides.write_text('{"patients": {"subject_id": "VARCHAR"}}', encoding="utf-8")
            db_path = base / "out.duckdb"

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--core-dir",
                    str(data_dir),
                    "--raw-core-schema",
                    "raw_core",
                    "--type-overrides",
                    str(overrides),
                    "--manifest",
                    "fail",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                val = con.execute("SELECT subject_id FROM raw_core.patients").fetchone()[0]
                self.assertEqual(val, "001")
            finally:
                con.close()
