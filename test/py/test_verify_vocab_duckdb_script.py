from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import duckdb


class TestVerifyVocabDuckDBScript(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.script = self.repo_root / "scripts" / "verify_vocab_duckdb.py"
        self.schema_dir = self.repo_root / "vocabulary_refresh" / "omop_schemas_vocab_bq"

    def _map_type(self, bq_type: str) -> str:
        return {
            "INT64": "BIGINT",
            "STRING": "VARCHAR",
            "FLOAT64": "DOUBLE",
            "DATE": "DATE",
            "TIMESTAMP": "TIMESTAMP",
        }.get(bq_type.upper(), "VARCHAR")

    def _make_db(self, schema: str, include_all: bool) -> Path:
        tmp_dir = Path(tempfile.mkdtemp())
        db_path = tmp_dir / "v.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
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
            if table != "concept" and not include_all:
                continue
            cols = (self.schema_dir / f"{table}.json").read_text(encoding="utf-8")
            import json

            columns = json.loads(cols)
            col_defs = [f"{c['name']} {self._map_type(c['type'])}" for c in columns]
            con.execute(f"CREATE TABLE {schema}.{table} ({', '.join(col_defs)})")
        con.close()
        return db_path

    def test_script_exits_zero_when_all_tables_present(self) -> None:
        db_path = self._make_db("main", include_all=True)
        res = subprocess.run(
            ["python3", str(self.script), "--database", str(db_path), "--schema", "main"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res.returncode, 0, msg=res.stderr)

    def test_script_exits_nonzero_when_missing_tables(self) -> None:
        db_path = self._make_db("main", include_all=False)
        res = subprocess.run(
            ["python3", str(self.script), "--database", str(db_path), "--schema", "main"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res.returncode, 1, msg=res.stderr)

    def test_script_fails_when_concept_rowcount_below_threshold(self) -> None:
        db_path = self._make_db("main", include_all=True)
        res = subprocess.run(
            [
                "python3",
                str(self.script),
                "--database",
                str(db_path),
                "--schema",
                "main",
                "--min-concept-rows",
                "1",
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res.returncode, 3, msg=res.stderr)
