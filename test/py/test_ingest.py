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
            con.execute("CREATE SCHEMA raw_hosp")
            con.execute("CREATE TABLE raw_hosp.admissions AS SELECT 1 AS x")

            self.ingest.load_folder(con, data_dir, "raw_hosp", lowercase=True, sample_size=-1)

            row = con.execute(
                """
                SELECT table_type
                FROM information_schema.tables
                WHERE table_schema='raw_hosp' AND table_name='admissions'
                """
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "BASE TABLE")
            self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_hosp.admissions").fetchone()[0], 1)
            con.close()

    def test_create_schema_views_replaces_existing_target_relations(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n1\n", encoding="utf-8")

            db_path = base / "test.duckdb"
            con = duckdb.connect(str(db_path))
            self.ingest.load_folder(con, data_dir, "raw_hosp", lowercase=True, sample_size=-1)

            # First create alias schema views
            self.ingest.create_schema_views(con, "raw_hosp", "raw_icu")
            table_type = con.execute(
                """
                SELECT table_type
                FROM information_schema.tables
                WHERE table_schema='raw_icu' AND table_name='patients'
                """
            ).fetchone()[0]
            self.assertEqual(table_type, "VIEW")

            # Simulate a stale table left behind, then re-run and ensure it becomes a view again.
            con.execute("DROP VIEW raw_icu.patients")
            con.execute("CREATE TABLE raw_icu.patients AS SELECT 123 AS subject_id")
            self.ingest.create_schema_views(con, "raw_hosp", "raw_icu")
            table_type2 = con.execute(
                """
                SELECT table_type
                FROM information_schema.tables
                WHERE table_schema='raw_icu' AND table_name='patients'
                """
            ).fetchone()[0]
            self.assertEqual(table_type2, "VIEW")
            self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_icu.patients").fetchone()[0], 1)
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
                    "--hosp-dir",
                    str(archive),
                    "--raw-hosp-schema",
                    "raw_hosp",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con = duckdb.connect(str(db_path), read_only=True)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_hosp.admissions").fetchone()[0], 1)
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
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
                    "--mode",
                    "copy",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con = duckdb.connect(str(db_path), read_only=True)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_hosp.patients").fetchone()[0], 1)
            con.close()

    def test_ingest_append_mode_appends_rows(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n2\n", encoding="utf-8")

            db_path = base / "out.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA raw_hosp")
            con.execute("CREATE TABLE raw_hosp.patients(subject_id INTEGER)")
            con.execute("INSERT INTO raw_hosp.patients VALUES (1)")
            con.close()

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
                    "--on-exists",
                    "append",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con2 = duckdb.connect(str(db_path), read_only=True)
            self.assertEqual(con2.execute("SELECT COUNT(*) FROM raw_hosp.patients").fetchone()[0], 2)
            con2.close()

    def test_ingest_drop_raw_clears_existing_raw_schemas(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n2\n", encoding="utf-8")

            db_path = base / "out.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA raw_hosp")
            con.execute("CREATE TABLE raw_hosp.patients(subject_id INTEGER)")
            con.execute("INSERT INTO raw_hosp.patients VALUES (1)")
            con.close()

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
                    "--drop-raw",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con2 = duckdb.connect(str(db_path), read_only=True)
            # After dropping raw schemas, only the ingested row should remain.
            self.assertEqual(con2.execute("SELECT COUNT(*) FROM raw_hosp.patients").fetchone()[0], 1)
            self.assertEqual(con2.execute("SELECT MIN(subject_id) FROM raw_hosp.patients").fetchone()[0], 2)
            con2.close()

    def test_ingest_manifest_fails_when_missing_required_tables(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            # Intentionally omit most required hosp tables.
            (data_dir / "patients.csv").write_text("subject_id\n1\n", encoding="utf-8")
            db_path = base / "out.duckdb"
            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
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
            data_dir = base / "hosp"
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
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
                    "--all-varchar",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                dtype = con.execute("SELECT data_type FROM information_schema.columns WHERE table_schema='raw_hosp' AND table_name='patients' AND column_name='subject_id'").fetchone()[0]
                self.assertIn("VARCHAR", dtype.upper())
                val = con.execute("SELECT subject_id FROM raw_hosp.patients").fetchone()[0]
                self.assertEqual(val, "001")
            finally:
                con.close()

    def test_ingest_type_overrides_preserve_leading_zeros(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
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
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
                    "--type-overrides",
                    str(overrides),
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                val = con.execute("SELECT subject_id FROM raw_hosp.patients").fetchone()[0]
                self.assertEqual(val, "001")
            finally:
                con.close()

    def test_ingest_row_limit_limits_rows_per_table(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n1\n2\n3\n4\n5\n", encoding="utf-8")
            (data_dir / "admissions.csv").write_text(
                "hadm_id,subject_id\n10,1\n11,2\n12,3\n13,4\n14,5\n", encoding="utf-8"
            )
            (data_dir / "transfers.csv").write_text(
                "transfer_id,hadm_id,subject_id\n1,10,1\n2,11,2\n3,12,3\n4,13,4\n5,14,5\n",
                encoding="utf-8",
            )
            db_path = base / "out.duckdb"

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
                    "--row-limit",
                    "2",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_hosp.patients").fetchone()[0], 2)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_hosp.admissions").fetchone()[0], 2)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_hosp.transfers").fetchone()[0], 2)
            finally:
                con.close()

    def test_ingest_row_limit_rejects_copy_mode(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n1\n2\n3\n", encoding="utf-8")
            (data_dir / "admissions.csv").write_text("hadm_id,subject_id\n10,1\n", encoding="utf-8")
            (data_dir / "transfers.csv").write_text("transfer_id,hadm_id,subject_id\n1,10,1\n", encoding="utf-8")
            db_path = base / "out.duckdb"

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
                    "--mode",
                    "copy",
                    "--row-limit",
                    "2",
                ],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(res.returncode, 0)

    def test_ingest_include_tables_only_loads_selected(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id\n1\n2\n", encoding="utf-8")
            (data_dir / "admissions.csv").write_text("hadm_id,subject_id\n10,1\n11,2\n", encoding="utf-8")
            (data_dir / "transfers.csv").write_text(
                "transfer_id,hadm_id,subject_id\n1,10,1\n2,11,2\n", encoding="utf-8"
            )
            db_path = base / "out.duckdb"

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
                    "--include-tables",
                    "patients,transfers",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                rows = con.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='raw_hosp' AND table_type='BASE TABLE'
                    ORDER BY table_name
                    """
                ).fetchall()
                self.assertEqual([r[0] for r in rows], ["patients", "transfers"])
            finally:
                con.close()

    def test_ingest_uses_official_ddl_types_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            data_dir = base / "hosp"
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / "patients.csv").write_text("subject_id,gender,anchor_year_group,anchor_year\n1,M,2000 - 2020,2020\n", encoding="utf-8")
            (data_dir / "admissions.csv").write_text(
                "subject_id,hadm_id,admittime,admission_type\n1,10,2020-01-01 00:00:00,EMERGENCY\n",
                encoding="utf-8",
            )
            (data_dir / "labevents.csv").write_text(
                "labevent_id,subject_id,specimen_id,itemid,charttime\n1,1,1,50868,2020-01-01 00:00:00\n",
                encoding="utf-8",
            )
            db_path = base / "out.duckdb"

            res = subprocess.run(
                [
                    "python3",
                    str(self.repo_root / "scripts" / "ingest_mimic_csv_to_duckdb.py"),
                    "--database",
                    str(db_path),
                    "--hosp-dir",
                    str(data_dir),
                    "--raw-hosp-schema",
                    "raw_hosp",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                patient_dtype = con.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema='raw_hosp' AND table_name='patients' AND column_name='subject_id'
                    """
                ).fetchone()[0]
                self.assertIn("INTEGER", patient_dtype.upper())

                admit_dtype = con.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema='raw_hosp' AND table_name='admissions' AND column_name='admittime'
                    """
                ).fetchone()[0]
                self.assertIn("TIMESTAMP", admit_dtype.upper())

                lab_dtype = con.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema='raw_hosp' AND table_name='labevents' AND column_name='charttime'
                    """
                ).fetchone()[0]
                self.assertIn("TIMESTAMP", lab_dtype.upper())
            finally:
                con.close()
