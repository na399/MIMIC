"""Build mock data and execute the DuckDB ETL using conf/mock.etlconf."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).parent
ROOT = CURRENT_DIR.parent.parent


def main() -> None:
    (ROOT / "data/mock_mimic.duckdb").unlink(missing_ok=True)
    subprocess.check_call([sys.executable, str(CURRENT_DIR / "generate_mock_data.py")], cwd=ROOT)
    subprocess.check_call(
        [sys.executable, "scripts/run_workflow.py", "-e", "conf/mock.etlconf"], cwd=ROOT
    )


if __name__ == "__main__":
    main()
