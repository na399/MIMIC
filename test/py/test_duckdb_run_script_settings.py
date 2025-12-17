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


class TestDuckDBRunScriptSettings(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.runner = load_module(self.repo_root / "scripts" / "duckdb_run_script.py", "duckdb_run_script")

    def test_threads_setting_is_applied_from_config(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "t.duckdb"
            sql_path = base / "s.sql"
            sql_path.write_text(
                "CREATE OR REPLACE TABLE settings AS SELECT current_setting('threads')::INTEGER AS threads;",
                encoding="utf-8",
            )
            config = {
                "variables": {"@duckdb_path": str(db_path)},
                "duckdb": {"database": "@duckdb_path", "threads": 1},
            }
            self.runner.run_scripts([str(sql_path)], config)
            con = duckdb.connect(str(db_path), read_only=True)
            try:
                self.assertEqual(con.execute("SELECT threads FROM settings").fetchone()[0], 1)
            finally:
                con.close()

