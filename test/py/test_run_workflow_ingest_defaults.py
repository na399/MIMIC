from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


def load_module(path: Path, name: str):
    scripts_dir = path.parent
    if str(scripts_dir) not in sys.path:
        sys.path.insert(0, str(scripts_dir))
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestRunWorkflowIngestDefaults(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.runner = load_module(self.repo_root / "scripts" / "run_workflow.py", "run_workflow_mod")

    def test_apply_ingest_defaults_sets_optional_variables(self) -> None:
        conf: dict = {"variables": {}}
        self.runner.apply_ingest_defaults(conf)
        variables = conf["variables"]
        for key in (
            "@ingest_sample_size",
            "@ingest_mode",
            "@ingest_manifest",
            "@ingest_on_exists",
            "@ingest_include_tables",
            "@ingest_row_limit",
            "@ingest_type_overrides",
            "@ingest_all_varchar_flag",
            "@ingest_drop_raw_flag",
            "@ingest_keep_case_flag",
        ):
            self.assertIn(key, variables)
            self.assertEqual(variables[key], "")

