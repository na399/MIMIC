from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import duckdb


class TestCrosswalkLoad(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.script = self.repo_root / "scripts" / "load_crosswalks_to_etl.py"

    def test_loads_d_items_crosswalk_into_etl_schema(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "etl.duckdb"
            cw_dir = base / "crosswalk_csv"
            cw_dir.mkdir(parents=True, exist_ok=True)
            (cw_dir / "d_items_to_concept.csv").write_text(
                "linksto,itemid,label,source_code,source_vocabulary_id,source_domain_id,source_concept_id,source_concept_name,target_vocabulary_id,target_domain_id,target_concept_id,target_concept_name,target_standard_concept\n"
                "chartevents,220045,Heart Rate,Heart Rate,mimiciv_meas_chart,Measurement,2000030001,Heart Rate,LOINC,Measurement,3027018,Heart rate,S\n",
                encoding="utf-8",
            )

            res = subprocess.run(
                [
                    "python3",
                    str(self.script),
                    "--database",
                    str(db_path),
                    "--etl-schema",
                    "omop_cdm",
                    "--crosswalk-dir",
                    str(cw_dir),
                ],
                capture_output=True,
                text=True,
                cwd=self.repo_root,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)

            con = duckdb.connect(str(db_path), read_only=True)
            try:
                self.assertEqual(
                    con.execute("SELECT COUNT(*) FROM omop_cdm.crosswalk_d_items_to_concept").fetchone()[0], 1
                )
            finally:
                con.close()

