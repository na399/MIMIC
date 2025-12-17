from __future__ import annotations

import unittest
from pathlib import Path


class TestNoInlineTaskMarkers(unittest.TestCase):
    def test_repo_has_no_task_markers_outside_todo_md(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        marker = "TO" + "DO"
        todo_md = "TO" + "DO.md"
        offenders: list[str] = []
        for path in repo_root.rglob("*"):
            if not path.is_file():
                continue
            if any(part in {".git", ".venv", "__pycache__"} for part in path.parts):
                continue
            if path.name == todo_md:
                continue
            # Skip binary-ish files.
            if path.suffix.lower() in {".duckdb", ".pdf", ".png", ".jpg", ".jpeg", ".gif"}:
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            if marker in text:
                offenders.append(str(path.relative_to(repo_root)))

        self.assertEqual(offenders, [], msg="Found task markers in: " + ", ".join(offenders))
