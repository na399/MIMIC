from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import duckdb


def map_type(bq_type: str) -> str:
    return {
        "INT64": "BIGINT",
        "STRING": "VARCHAR",
        "FLOAT64": "DOUBLE",
        "DATE": "DATE",
        "TIMESTAMP": "TIMESTAMP",
    }.get(bq_type.upper(), "VARCHAR")


class TestVocabSnapshot(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.schema_dir = self.repo_root / "vocabulary_refresh" / "omop_schemas_vocab_bq"
        self.script = self.repo_root / "scripts" / "create_vocab_snapshot.py"

    def _create_full_vocab(self, db_path: Path, schema: str = "main") -> None:
        con = duckdb.connect(str(db_path))
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for table in (
            "concept",
            "concept_relationship",
            "vocabulary",
            "domain",
            "concept_class",
            "relationship",
            "concept_synonym",
            "concept_ancestor",
            "drug_strength",
        ):
            cols = json.loads((self.schema_dir / f"{table}.json").read_text(encoding="utf-8"))
            col_defs = [f"{c['name']} {map_type(c['type'])}" for c in cols]
            con.execute(f"CREATE TABLE {schema}.{table} ({', '.join(col_defs)})")

        # Insert a couple concepts and a mapping relationship.
        con.execute(
            f"""
            INSERT INTO {schema}.concept
            VALUES
              (100, 'A', 'X', 'V1', 'C1', 'S', 'A', DATE '1970-01-01', DATE '2099-12-31', NULL),
              (200, 'B', 'X', 'V1', 'C1', NULL, 'B', DATE '1970-01-01', DATE '2099-12-31', NULL),
              (300, 'C', 'X', 'V1', 'C1', 'S', 'C', DATE '1970-01-01', DATE '2099-12-31', NULL)
            """
        )
        con.execute(
            f"""
            INSERT INTO {schema}.concept_relationship
            VALUES
              (200, 100, 'Maps to', DATE '1970-01-01', DATE '2099-12-31', NULL),
              (300, 100, 'Maps to', DATE '1970-01-01', DATE '2099-12-31', NULL)
            """
        )
        con.close()

    def test_snapshot_keeps_only_requested_concept_ids(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            full_db = base / "full.duckdb"
            out_db = base / "snap.duckdb"
            self._create_full_vocab(full_db, schema="main")

            res = subprocess.run(
                [
                    "python3",
                    str(self.script),
                    "--full-db",
                    str(full_db),
                    "--out-db",
                    str(out_db),
                    "--schema",
                    "main",
                    "--concept-id",
                    "100",
                    "--concept-id",
                    "200",
                ],
                capture_output=True,
                text=True,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)

            con = duckdb.connect(str(out_db), read_only=True)
            try:
                ids = [r[0] for r in con.execute("SELECT concept_id FROM main.concept ORDER BY concept_id").fetchall()]
                self.assertEqual(ids, [100, 200])
                rels = con.execute(
                    "SELECT concept_id_1, concept_id_2 FROM main.concept_relationship ORDER BY concept_id_1, concept_id_2"
                ).fetchall()
                self.assertEqual(rels, [(200, 100)])
                # Required tables still exist.
                present = {
                    r[0]
                    for r in con.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                    ).fetchall()
                }
                self.assertTrue({"concept", "concept_relationship", "vocabulary", "domain"}.issubset(present))
            finally:
                con.close()

