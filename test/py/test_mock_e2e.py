from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

import duckdb


class TestMockE2E(unittest.TestCase):
    def test_mock_pipeline_runs_in_temp_paths_and_passes_audits(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        gen = repo_root / "test" / "mock_data" / "generate_mock_data.py"
        runner = repo_root / "scripts" / "run_workflow.py"

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            mimic_dir = base / "mock_mimic"
            vocab_db = base / "mock_vocab.duckdb"
            out_db = base / "mock_out.duckdb"
            crosswalk_dir = base / "crosswalk_csv"

            res_gen = subprocess.run(
                [
                    "python3",
                    str(gen),
                    "--base-dir",
                    str(mimic_dir),
                    "--vocab-db",
                    str(vocab_db),
                ],
                capture_output=True,
                text=True,
                cwd=repo_root,
            )
            self.assertEqual(res_gen.returncode, 0, msg=res_gen.stderr)
            self.assertTrue(vocab_db.exists())

            args = [
                "python3",
                str(runner),
                "-e",
                "conf/mock.etlconf",
                "--set",
                f"@duckdb_path={out_db}",
                "--set",
                f"@mimic_core_dir={mimic_dir / 'core'}",
                "--set",
                f"@mimic_hosp_dir={mimic_dir / 'hosp'}",
                "--set",
                f"@mimic_icu_dir={mimic_dir / 'icu'}",
                "--set",
                f"@mimic_derived_dir={mimic_dir / 'derived'}",
                "--set",
                f"@mimic_waveform_dir={mimic_dir / 'waveform'}",
                "--set",
                f"@vocab_db_path={vocab_db}",
                "--set",
                f"@crosswalk_dir={crosswalk_dir}",
                "--set",
                "@vocab_schema=main",
                "--set",
                "@voc_dataset=main",
            ]
            res = subprocess.run(args, capture_output=True, text=True, cwd=repo_root)
            self.assertEqual(res.returncode, 0, msg=res.stderr[-2000:])

            con = duckdb.connect(str(out_db), read_only=True)
            try:
                fails = con.execute("SELECT COUNT(*) FROM audit.table_population WHERE status='FAIL'").fetchone()[0]
                self.assertEqual(fails, 0)
                self.assertGreater(con.execute("SELECT COUNT(*) FROM omop.condition_occurrence").fetchone()[0], 0)
                bad_names = con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='omop' AND table_name LIKE 'cdm_%'"
                ).fetchone()[0]
                self.assertEqual(bad_names, 0)
                bigints = con.execute(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema='omop' AND data_type IN ('BIGINT','HUGEINT','UBIGINT')"
                ).fetchone()[0]
                self.assertEqual(bigints, 0)
                self.assertGreater(con.execute("SELECT COUNT(*) FROM audit.mapping_rate").fetchone()[0], 0)
                self.assertGreater(con.execute("SELECT COUNT(*) FROM audit.unmapped_top").fetchone()[0], 0)
                cond_map = con.execute(
                    "SELECT mapped_rows, standard_rows FROM audit.mapping_rate "
                    "WHERE table_name='condition_occurrence' AND concept_field='condition_concept_id'"
                ).fetchone()
                self.assertIsNotNone(cond_map)
                self.assertGreater(cond_map[0], 0)
                self.assertGreater(cond_map[1], 0)
                proc_map = con.execute(
                    "SELECT mapped_rows, standard_rows FROM audit.mapping_rate "
                    "WHERE table_name='procedure_occurrence' AND concept_field='procedure_concept_id'"
                ).fetchone()
                self.assertIsNotNone(proc_map)
                self.assertGreater(proc_map[0], 0)
                self.assertGreater(proc_map[1], 0)
                drug_map = con.execute(
                    "SELECT mapped_rows, standard_rows FROM audit.mapping_rate "
                    "WHERE table_name='drug_exposure' AND concept_field='drug_concept_id'"
                ).fetchone()
                self.assertIsNotNone(drug_map)
                self.assertGreater(drug_map[0], 0)
                self.assertGreater(drug_map[1], 0)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM audit.run_metadata").fetchone()[0], 1)
                self.assertEqual(
                    con.execute("SELECT COUNT(*) FROM audit.omop_schema_validation WHERE status='FAIL'").fetchone()[0],
                    0,
                )
                self.assertEqual(con.execute("SELECT COUNT(*) FROM audit.dq_checks WHERE status='FAIL'").fetchone()[0], 0)
            finally:
                con.close()

    def test_mock_pipeline_fails_when_mapping_thresholds_are_too_high(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        gen = repo_root / "test" / "mock_data" / "generate_mock_data.py"
        runner = repo_root / "scripts" / "run_workflow.py"

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            mimic_dir = base / "mock_mimic"
            vocab_db = base / "mock_vocab.duckdb"
            out_db = base / "mock_out.duckdb"
            crosswalk_dir = base / "crosswalk_csv"

            res_gen = subprocess.run(
                ["python3", str(gen), "--base-dir", str(mimic_dir), "--vocab-db", str(vocab_db)],
                capture_output=True,
                text=True,
                cwd=repo_root,
            )
            self.assertEqual(res_gen.returncode, 0, msg=res_gen.stderr)

            res = subprocess.run(
                [
                    "python3",
                    str(runner),
                    "-e",
                    "conf/mock.etlconf",
                    "--set",
                    f"@duckdb_path={out_db}",
                    "--set",
                    f"@mimic_core_dir={mimic_dir / 'core'}",
                    "--set",
                    f"@mimic_hosp_dir={mimic_dir / 'hosp'}",
                    "--set",
                    f"@mimic_icu_dir={mimic_dir / 'icu'}",
                    "--set",
                    f"@mimic_derived_dir={mimic_dir / 'derived'}",
                    "--set",
                    f"@mimic_waveform_dir={mimic_dir / 'waveform'}",
                    "--set",
                    f"@vocab_db_path={vocab_db}",
                    "--set",
                    f"@crosswalk_dir={crosswalk_dir}",
                    "--set",
                    "@vocab_schema=main",
                    "--set",
                    "@voc_dataset=main",
                    "--set",
                    "@audit_min_percent_mapped=99",
                ],
                capture_output=True,
                text=True,
                cwd=repo_root,
            )
            self.assertNotEqual(res.returncode, 0)

    def test_mock_pipeline_fails_with_no_clinical_maps_under_default_thresholds(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        gen = repo_root / "test" / "mock_data" / "generate_mock_data.py"
        runner = repo_root / "scripts" / "run_workflow.py"

        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            mimic_dir = base / "mock_mimic"
            vocab_db = base / "mock_vocab.duckdb"
            out_db = base / "mock_out.duckdb"
            crosswalk_dir = base / "crosswalk_csv"

            res_gen = subprocess.run(
                ["python3", str(gen), "--base-dir", str(mimic_dir), "--vocab-db", str(vocab_db), "--no-clinical-maps"],
                capture_output=True,
                text=True,
                cwd=repo_root,
            )
            self.assertEqual(res_gen.returncode, 0, msg=res_gen.stderr)

            res = subprocess.run(
                [
                    "python3",
                    str(runner),
                    "-e",
                    "conf/mock.etlconf",
                    "--set",
                    f"@duckdb_path={out_db}",
                    "--set",
                    f"@mimic_core_dir={mimic_dir / 'core'}",
                    "--set",
                    f"@mimic_hosp_dir={mimic_dir / 'hosp'}",
                    "--set",
                    f"@mimic_icu_dir={mimic_dir / 'icu'}",
                    "--set",
                    f"@mimic_derived_dir={mimic_dir / 'derived'}",
                    "--set",
                    f"@mimic_waveform_dir={mimic_dir / 'waveform'}",
                    "--set",
                    f"@vocab_db_path={vocab_db}",
                    "--set",
                    f"@crosswalk_dir={crosswalk_dir}",
                    "--set",
                    "@vocab_schema=main",
                    "--set",
                    "@voc_dataset=main",
                ],
                capture_output=True,
                text=True,
                cwd=repo_root,
            )
            self.assertNotEqual(res.returncode, 0, msg=res.stdout[-2000:] + "\n" + res.stderr[-2000:])
