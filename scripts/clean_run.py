"""
Archive an existing DuckDB output file before a new run.

This is a safety helper for reproducible ETL runs: instead of overwriting an
existing `*.duckdb`, move it aside to an archive folder with a timestamp.
"""

from __future__ import annotations

import argparse
import shutil
from datetime import datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive an existing DuckDB output file")
    parser.add_argument("--path", required=True, help="Path to DuckDB file to archive (if it exists)")
    parser.add_argument("--archive-dir", default="data/archive", help="Folder to store archived DBs")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    src = Path(args.path)
    if not src.exists():
        print(f"No existing DB at {src}; nothing to do.")
        return 0
    if src.suffix.lower() != ".duckdb":
        raise ValueError(f"Refusing to archive non-.duckdb file: {src}")

    archive_dir = Path(args.archive_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = archive_dir / f"{src.stem}.{ts}.duckdb"
    shutil.move(str(src), str(dst))
    print(f"Archived {src} -> {dst}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

