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


class TestVariableSubstitution(unittest.TestCase):
    def setUp(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        self.duck = load_module(repo_root / "scripts" / "duckdb_run_script.py", "duckdb_run_script_mod")

    def test_substitute_variables_replaces_all_occurrences(self) -> None:
        text = "x @foo y @foo z"
        out = self.duck.substitute_variables(text, {"@foo": "BAR"})
        self.assertEqual(out, "x BAR y BAR z")

    def test_merge_config_deep_merges_dicts(self) -> None:
        base = {"a": {"b": 1, "c": 2}, "x": 1}
        override = {"a": {"c": 3}, "y": 2}
        merged = self.duck.merge_config(base, override)
        self.assertEqual(merged["a"]["b"], 1)
        self.assertEqual(merged["a"]["c"], 3)
        self.assertEqual(merged["x"], 1)
        self.assertEqual(merged["y"], 2)

