from __future__ import annotations

import unittest
from pathlib import Path


class TestNoCoreModuleReferences(unittest.TestCase):
    def test_repo_has_no_core_module_markers(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        markers = (
            "@mimic_" + "core_dir",
            "@" + "core_dataset",
            "--" + "core-dir",
            "raw_" + "core",
            "st_" + "core.sql",
        )

        offenders: list[str] = []
        for path in repo_root.rglob("*"):
            if not path.is_file():
                continue
            if any(part in {".git", ".venv", "__pycache__"} for part in path.parts):
                continue
            # Skip binary-ish files.
            if path.suffix.lower() in {".duckdb", ".pdf", ".png", ".jpg", ".jpeg", ".gif"}:
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            if any(m in text for m in markers):
                offenders.append(str(path.relative_to(repo_root)))

        self.assertEqual(offenders, [], msg="Found core-module references in: " + ", ".join(offenders))
