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


class TestVocabVerify(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.run_workflow = load_module(self.repo_root / "scripts" / "run_workflow.py", "run_workflow")

    def _create_vocab_db(self, schema: str, missing: set[str] | None = None) -> Path:
        required = set(self.run_workflow.REQUIRED_VOCAB_TABLES)
        if missing:
            required -= set(missing)
        tmp_dir = Path(tempfile.mkdtemp())
        db_path = tmp_dir / "tmp_vocab.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for table in sorted(required):
            con.execute(f"CREATE TABLE {schema}.{table} AS SELECT 1 AS dummy LIMIT 0")
        con.close()
        return db_path

    def test_apply_vocab_defaults_detects_legacy_vocab_schema(self) -> None:
        db_path = self._create_vocab_db("vocab")
        etlconf = {"variables": {"@vocab_db_path": str(db_path)}}
        self.run_workflow.apply_vocab_defaults(etlconf)
        self.assertEqual(etlconf["variables"]["@vocab_schema"], "vocab")
        self.assertEqual(etlconf["variables"]["@voc_dataset"], "vocab")

    def test_verify_vocab_tables_passes_when_all_present(self) -> None:
        db_path = self._create_vocab_db("main")
        etlconf = {"variables": {"@vocab_db_path": str(db_path), "@vocab_schema": "main", "@voc_dataset": "main"}}
        self.run_workflow.verify_vocab_tables(etlconf)

    def test_verify_vocab_tables_raises_when_missing_required_table(self) -> None:
        db_path = self._create_vocab_db("main", missing={"concept_relationship"})
        etlconf = {"variables": {"@vocab_db_path": str(db_path), "@vocab_schema": "main", "@voc_dataset": "main"}}
        with self.assertRaises(RuntimeError):
            self.run_workflow.verify_vocab_tables(etlconf)
