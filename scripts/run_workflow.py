"""
Lightweight workflow runner for the DuckDB-first MIMIC-IV -> OMOP CDM v5.4 ETL.

The runner reads a global ETL configuration (``-e``), optionally merges a
workflow-level override (``-c``), and then executes each workflow described in
the config. SQL workflows are executed via ``scripts/duckdb_run_script.py`` and
Python workflows are executed directly with the configured arguments.
"""
import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple
from uuid import uuid4

import duckdb
from duckdb_run_script import run_scripts, merge_config, substitute_variables
from tqdm import tqdm


REQUIRED_VOCAB_TABLES = (
    "concept",
    "concept_relationship",
    "vocabulary",
    "domain",
    "concept_class",
    "relationship",
    "concept_synonym",
    "concept_ancestor",
    "drug_strength",
)


def load_json(path: str) -> Dict[str, Any]:
    if not path:
        return {}
    file_path = Path(path)
    return json.loads(file_path.read_text()) if file_path.exists() else {}


def timestamped(message: str) -> str:
    return f"[{datetime.now().strftime('%H:%M:%S')}] {message}"


def apply_variables(obj: Any, variables: Dict[str, str]) -> Any:
    if isinstance(obj, str):
        return substitute_variables(obj, variables)
    if isinstance(obj, list):
        return [apply_variables(item, variables) for item in obj]
    if isinstance(obj, dict):
        return {k: apply_variables(v, variables) for k, v in obj.items()}
    return obj


def run_python_script(script: Dict[str, Any], variables: Dict[str, str], log_func) -> None:
    args = [sys.executable, script["script"]]
    for extra in script.get("args", []):
        value = substitute_variables(extra, variables)
        if value == "":
            # Common pattern in workflow configs is ["--flag", "@maybe_empty"].
            # If the value is empty, drop both the value and its preceding flag
            # to avoid passing an option with a missing argument.
            if args and args[-1].startswith("-"):
                args.pop()
            continue
        args.append(value)
    log_func(timestamped(f"Running python: {' '.join(args)}"))
    subprocess.check_call(args)


def resolve_workflow(etlconf: Dict[str, Any], workflow_entry: Dict[str, Any], config_dir: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]], str, str]:
    workflow_conf_path = config_dir / workflow_entry.get("conf", "")
    workflow_conf = load_json(str(workflow_conf_path))

    variables = merge_config(etlconf.get("variables", {}), workflow_conf.get("variables", {}))
    merged_conf = merge_config(etlconf, workflow_conf)
    merged_conf["variables"] = variables
    merged_conf["duckdb"] = apply_variables(merged_conf.get("duckdb", {}), variables)

    scripts: List[Dict[str, Any]] = merged_conf.get("scripts", [])
    workflow_type = merged_conf.get("type")
    name = workflow_entry.get("name") or workflow_conf.get("name") or workflow_conf_path.stem
    return merged_conf, scripts, workflow_type, name


def run_workflow(
    merged_conf: Dict[str, Any],
    scripts: List[Dict[str, Any]],
    workflow_type: str,
    workflow_name: str,
    progress: tqdm,
) -> None:
    log_func = progress.write
    log_func(timestamped(f"Starting workflow '{workflow_name}' with {len(scripts)} step(s)"))

    if workflow_type == "sql":
        run_scripts(
            [s["script"] for s in scripts],
            merged_conf,
            progress_cb=lambda: progress.update(1),
            log=log_func,
        )
    elif workflow_type == "py":
        for script in scripts:
            run_python_script(script, merged_conf.get("variables", {}), log_func)
            progress.update(1)
    else:
        raise ValueError(f"Unsupported workflow type: {workflow_type}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MIMIC-IV -> OMOP ETL workflows")
    parser.add_argument("-e", "--etlconf", dest="etlconf", required=True, help="Global ETL config json")
    parser.add_argument("-c", "--config", dest="config", help="Workflow override json")
    parser.add_argument(
        "--set",
        dest="variable_overrides",
        action="append",
        default=[],
        metavar="VAR=VALUE",
        help="Override ETL variables (repeatable), e.g. --set @vocab_db_path=data/vocab.duckdb",
    )
    return parser.parse_args()


def _schema_has_table(db_path: Path, schema: str, table: str) -> bool:
    if not db_path.exists():
        return False
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            LIMIT 1
            """,
            [schema, table],
        ).fetchone()
        return row is not None
    finally:
        con.close()


def apply_vocab_defaults(etlconf: Dict[str, Any]) -> None:
    variables = etlconf.setdefault("variables", {})

    # Back-compat: vocab_smoke has been consolidated into mock_vocab.
    vocab_db_raw = str(variables.get("@vocab_db_path", "") or "")
    if vocab_db_raw.endswith("vocab_smoke.duckdb"):
        variables["@vocab_db_path"] = "data/mock_vocab.duckdb"
        vocab_db_raw = "data/mock_vocab.duckdb"

    vocab_db = Path(vocab_db_raw) if vocab_db_raw else None
    full_vocab = Path("data/vocab.duckdb")
    mock_vocab = Path("data/mock_vocab.duckdb")

    if vocab_db is None or vocab_db_raw.strip() == "":
        variables["@vocab_db_path"] = str(full_vocab if full_vocab.exists() else mock_vocab)
        vocab_db = Path(variables["@vocab_db_path"])
    elif not vocab_db.exists():
        if full_vocab.exists():
            variables["@vocab_db_path"] = str(full_vocab)
            vocab_db = full_vocab
        elif mock_vocab.exists():
            variables["@vocab_db_path"] = str(mock_vocab)
            vocab_db = mock_vocab
        else:
            raise FileNotFoundError(f"Vocab DuckDB not found: {vocab_db_raw}")

    # Default to "main" schema, but fall back to legacy "vocab" if needed.
    variables.setdefault("@vocab_schema", "main")
    variables.setdefault("@voc_dataset", variables.get("@vocab_schema", "main"))

    schema = str(variables.get("@voc_dataset") or variables.get("@vocab_schema") or "main")
    if not _schema_has_table(vocab_db, schema, "concept") and _schema_has_table(vocab_db, "vocab", "concept"):
        variables["@vocab_schema"] = "vocab"
        variables["@voc_dataset"] = "vocab"


def apply_audit_defaults(etlconf: Dict[str, Any]) -> None:
    variables = etlconf.setdefault("variables", {})
    variables.setdefault("@audit_min_percent_mapped", "0")
    variables.setdefault("@audit_min_percent_standard", "0")
    variables.setdefault("@audit_fail_on_dq", "0")
    variables.setdefault("@audit_mapping_tables", "")

    variables.setdefault("@run_started_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    variables.setdefault("@run_id", str(uuid4()))

    if "@git_sha" not in variables:
        try:
            repo_root = Path(__file__).resolve().parents[1]
            sha = (
                subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_root, stderr=subprocess.DEVNULL)
                .decode("utf-8")
                .strip()
            )
        except Exception:
            sha = ""
        variables["@git_sha"] = sha


def apply_ingest_defaults(etlconf: Dict[str, Any]) -> None:
    variables = etlconf.setdefault("variables", {})
    variables.setdefault("@ingest_sample_size", "")
    variables.setdefault("@ingest_mode", "")
    variables.setdefault("@ingest_manifest", "")
    variables.setdefault("@ingest_on_exists", "")
    variables.setdefault("@ingest_include_tables", "")
    variables.setdefault("@ingest_row_limit", "")
    variables.setdefault("@ingest_type_overrides", "")
    variables.setdefault("@ingest_all_varchar_flag", "")
    variables.setdefault("@ingest_drop_raw_flag", "")
    variables.setdefault("@ingest_keep_case_flag", "")

def apply_optimize_defaults(etlconf: Dict[str, Any]) -> None:
    variables = etlconf.setdefault("variables", {})
    variables.setdefault("@optimize_enable", "0")
    variables.setdefault("@optimize_enable_large", "0")


def verify_vocab_tables(etlconf: Dict[str, Any]) -> None:
    variables = etlconf.get("variables", {}) or {}
    vocab_db_path = Path(str(variables.get("@vocab_db_path") or ""))
    schema = str(variables.get("@voc_dataset") or variables.get("@vocab_schema") or "main")
    if not vocab_db_path.exists():
        raise FileNotFoundError(vocab_db_path)

    con = duckdb.connect(str(vocab_db_path), read_only=True)
    try:
        missing: list[str] = []
        for table in REQUIRED_VOCAB_TABLES:
            row = con.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = ? AND table_name = ?
                LIMIT 1
                """,
                [schema, table],
            ).fetchone()
            if row is None:
                missing.append(table)
        if missing:
            raise RuntimeError(
                "Vocab DuckDB is missing required tables in schema "
                f"'{schema}': {', '.join(missing)}. "
                "Point @vocab_db_path at a full OMOP vocab DB (data/vocab.duckdb) "
                "or bootstrap your minimal DB with scripts/bootstrap_vocab_smoke.py."
            )
    finally:
        con.close()


def main() -> int:
    args = parse_args()
    etlconf_path = Path(args.etlconf)
    etlconf = load_json(str(etlconf_path))
    override_conf = load_json(args.config)
    merged_etlconf = merge_config(etlconf, override_conf)

    if args.variable_overrides:
        merged_etlconf.setdefault("variables", {})
        for raw in args.variable_overrides:
            if "=" not in raw:
                raise ValueError(f"Invalid --set value (expected VAR=VALUE): {raw}")
            key, value = raw.split("=", 1)
            key = key.strip()
            merged_etlconf["variables"][key] = value

    apply_vocab_defaults(merged_etlconf)
    apply_audit_defaults(merged_etlconf)
    apply_ingest_defaults(merged_etlconf)
    apply_optimize_defaults(merged_etlconf)
    verify_vocab_tables(merged_etlconf)

    config_dir = etlconf_path.parent
    workflows = merged_etlconf.get("workflows", [])
    if not workflows:
        raise ValueError("No workflows defined in ETL config")

    resolved = [resolve_workflow(merged_etlconf, wf, config_dir) for wf in workflows]
    total_steps = sum(len(scripts) for _, scripts, _, _ in resolved)

    with tqdm(total=total_steps, desc="ETL pipeline", unit="step") as progress:
        for merged_conf, scripts, workflow_type, workflow_name in resolved:
            run_workflow(merged_conf, scripts, workflow_type, workflow_name, progress)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
