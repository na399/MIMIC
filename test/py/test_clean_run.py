from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path


class TestCleanRun(unittest.TestCase):
    def setUp(self) -> None:
        self.repo_root = Path(__file__).resolve().parents[2]
        self.script = self.repo_root / "scripts" / "clean_run.py"

    def test_archives_existing_duckdb_file(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            base = Path(td)
            db_path = base / "out.duckdb"
            db_path.write_text("dummy", encoding="utf-8")
            archive_dir = base / "archive"

            res = subprocess.run(
                ["python3", str(self.script), "--path", str(db_path), "--archive-dir", str(archive_dir)],
                capture_output=True,
                text=True,
                cwd=self.repo_root,
            )
            self.assertEqual(res.returncode, 0, msg=res.stderr)
            self.assertFalse(db_path.exists())
            archived = list(archive_dir.glob("out.*.duckdb"))
            self.assertEqual(len(archived), 1)

