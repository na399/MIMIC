from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import duckdb


class TestMaterializeOMOP(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.script = self.repo_root / "scripts" / "materialize_omop_schema.py"

    def test_materializes_views_into_tables_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "x.duckdb"
            con = duckdb.connect(str(db_path))
            con.execute("CREATE SCHEMA omop")
            con.execute("CREATE TABLE src AS SELECT 1 AS a")
            con.execute("CREATE VIEW omop.person AS SELECT * FROM src")
            con.close()

            res = subprocess.run(
                [
                    "python3",
                    str(self.script),
                    "--database",
                    str(db_path),
                    "--schema",
                    "omop",
                    "--enable",
                    "1",
                ],
                capture_output=True,
                text=True,
                cwd=self.repo_root,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)

            con2 = duckdb.connect(str(db_path), read_only=True)
            try:
                ttype = con2.execute(
                    "SELECT table_type FROM information_schema.tables WHERE table_schema='omop' AND table_name='person'"
                ).fetchone()[0]
                self.assertEqual(ttype, "BASE TABLE")
                self.assertEqual(con2.execute("SELECT COUNT(*) FROM omop.person").fetchone()[0], 1)
            finally:
                con2.close()

