"""
Load OMOP vocabulary into DuckDB using athena2duckdb.

This wrapper keeps configuration consistent with the rest of the ETL workflow
and avoids hard-coding command lines in workflow configs.
"""
import argparse
import importlib.util
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load OMOP vocab into DuckDB via athena2duckdb")
    parser.add_argument(
        "--athena-export",
        default="",
        help="Path to Athena export folder (optional; when omitted, no-op)",
    )
    parser.add_argument("--database", required=True, help="DuckDB file to write vocabulary into")
    parser.add_argument("--schema", default="vocab", help="Target schema inside the DuckDB file")
    parser.add_argument("--extra-args", nargs="*", default=[], help="Extra flags forwarded to athena2duckdb")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.athena_export:
        print("No --athena-export provided; skipping vocabulary refresh.")
        return 0

    export_path = Path(args.athena_export)
    if not export_path.exists():
        print(f"Athena export folder not found at {export_path}; skipping vocabulary refresh.")
        return 0

    if importlib.util.find_spec("athena2duckdb") is None:
        print(
            "Athena export folder found, but 'athena2duckdb' is not installed; skipping vocabulary refresh.\n"
            "Tip: keep using a prebuilt vocab DB (e.g., data/vocab.duckdb) or install athena2duckdb in your env."
        )
        return 0

    cmd = [
        sys.executable,
        "-m",
        "athena2duckdb",
        "load",
        str(export_path),
        "--database",
        args.database,
        "--schema",
        args.schema,
    ] + args.extra_args

    print("Executing:", " ".join(cmd))
    subprocess.check_call(cmd, cwd=export_path.resolve().parent)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
