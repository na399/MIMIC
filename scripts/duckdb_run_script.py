"""
Execute SQL scripts against DuckDB with variable substitution and light BigQuery-to-DuckDB
normalization so existing ETL assets can run locally.
"""
import argparse
import json
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

import duckdb


DEFAULT_CONFIG = {
    "variables": {},
    "duckdb": {
        "database": "data/mimiciv.duckdb",
        "attachments": [],
        "pre_sql": [],
        "max_expression_depth": 100000,
    },
}


def load_json(path: str) -> Dict[str, Any]:
    return json.loads(Path(path).read_text()) if path and Path(path).exists() else {}


def merge_config(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merged[key] = merge_config(base[key], value)
        else:
            merged[key] = value
    return merged


def substitute_variables(text: str, variables: Dict[str, str]) -> str:
    for var, val in variables.items():
        text = text.replace(var, val)
    return text


def split_queries(raw_sql: str) -> List[str]:
    queries = []
    current = []
    depth = 0
    for line in raw_sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        current.append(line)
        depth += line.count("(") - line.count(")")
        if ";" in line and depth <= 0:
            statement = "\n".join(current)
            before, _sep, _after = statement.partition(";")
            if before.strip():
                queries.append(before)
            current = []
    if current:
        queries.append("\n".join(current))
    return [q for q in queries if q.strip()]


def _split_struct_fields(payload: str) -> List[str]:
    fields: List[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(payload):
        if ch == "," and depth == 0:
            fields.append(payload[start:i])
            start = i + 1
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
    fields.append(payload[start:])
    return [f.strip() for f in fields if f.strip()]


def normalize_struct_json(sql: str) -> str:
    pattern = re.compile(r"TO_JSON_STRING\s*\(\s*STRUCT\((.*?)\)\s*\)", re.IGNORECASE | re.DOTALL)

    def _repl(match: re.Match) -> str:
        inner = match.group(1)
        parts = []
        for field in _split_struct_fields(inner):
            chunks = re.split(r"\s+AS\s+", field, flags=re.IGNORECASE)
            if len(chunks) == 2:
                parts.append(f"{chunks[1].strip()} := {chunks[0].strip()}")
            else:
                parts.append(field.strip())
        return f"to_json(struct_pack({', '.join(parts)}))"

    return pattern.sub(_repl, sql)


def normalize_query(sql: str) -> str:
    replacements = {
        "FARM_FINGERPRINT(GENERATE_UUID())": "(abs(hash(uuid())) % 9007199254740991)",
        "GENERATE_UUID()": "uuid()",
    }
    normalized = normalize_struct_json(sql)
    # Align BigQuery REGEXP_EXTRACT semantics (NULL when no match) without
    # shadowing DuckDB's built-in regexp_extract. Keep this replacement
    # case-sensitive so macro definitions that call DuckDB's regexp_extract()
    # are not rewritten into recursion.
    normalized = re.sub(r"\bREGEXP_EXTRACT\b", "BQ_REGEXP_EXTRACT", normalized)
    # Map BigQuery scalar types to DuckDB types without rewriting identifiers
    # that merely contain these substrings (e.g., TO_JSON_STRING).
    normalized = re.sub(r"\bSTRING\b", "VARCHAR", normalized)
    normalized = re.sub(r"\bINT64\b", "BIGINT", normalized)
    normalized = re.sub(r"\bFLOAT64\b", "DOUBLE", normalized)
    # DATETIME is used as a type in many legacy scripts, but also as a helper
    # function/macro (DATETIME(...)). Only rewrite the type usage.
    normalized = re.sub(r"\bDATETIME\b(?!\s*\()", "TIMESTAMP", normalized)
    normalized = normalized.replace("`", "")
    normalized = re.sub(r"r'([^']*)'", r"'\1'", normalized)
    for key, value in replacements.items():
        normalized = normalized.replace(key, value)
    normalized = re.sub(
        r"PARSE_DATE\s*\(\s*'[^']*'\s*,\s*'([^']+)'\s*\)",
        lambda m: f"DATE '{m.group(1)}'",
        normalized,
        flags=re.IGNORECASE,
    )
    return normalized


def execute_query(con: duckdb.DuckDBPyConnection, query: str) -> None:
    con.execute(query)


def run_scripts(
    script_files: Iterable[str],
    config: Dict[str, Any],
    *,
    progress_cb: Optional[Callable[[], None]] = None,
    log: Optional[Callable[[str], None]] = None,
) -> None:
    merged = merge_config(DEFAULT_CONFIG, config)
    duck_conf = merged.get("duckdb", {})
    variables = merged.get("variables", {})
    db_path = substitute_variables(str(duck_conf.get("database", DEFAULT_CONFIG["duckdb"]["database"])), variables)

    # DuckDB exposes the connected database as a catalog; for file-backed DBs
    # the default name is the file stem (see PRAGMA database_list).
    variables = {**variables, "@duckdb_catalog": Path(db_path).stem}

    con = duckdb.connect(db_path)

    threads = duck_conf.get("threads")
    if threads is not None and str(threads) != "":
        con.execute(f"SET threads={int(threads)}")

    memory_limit = duck_conf.get("memory_limit")
    if memory_limit is not None and str(memory_limit) != "":
        con.execute(f"SET memory_limit='{substitute_variables(str(memory_limit), variables)}'")

    temp_directory = duck_conf.get("temp_directory")
    if temp_directory is not None and str(temp_directory) != "":
        con.execute(f"SET temp_directory='{substitute_variables(str(temp_directory), variables)}'")

    max_depth = duck_conf.get("max_expression_depth")
    if max_depth:
        con.execute(f"SET max_expression_depth={max_depth}")

    for attach in duck_conf.get("attachments", []) or []:
        attach_path = substitute_variables(str(attach.get("path", "")), variables)
        alias = attach.get("alias") or Path(attach_path).stem
        read_only = attach.get("read_only", False)
        suffix = " (READ_ONLY)" if read_only else ""
        if log:
            log(f"Attaching {attach_path} as {alias}{suffix}")
        con.execute(f"ATTACH DATABASE '{attach_path}' AS {alias}{suffix}")

    for pre_sql in duck_conf.get("pre_sql", []) or []:
        pre_path = substitute_variables(str(pre_sql), variables)
        for idx, statement in enumerate(split_queries(Path(pre_path).read_text()), start=1):
            start_time = time.perf_counter()
            message = f"Executing {pre_path} [statement {idx}]"
            (log or print)(message)
            execute_query(con, substitute_variables(normalize_query(statement), variables))
            duration = time.perf_counter() - start_time
            if log:
                log(f"Finished {pre_path} [statement {idx}] in {duration:.2f}s")

    for script_file in script_files:
        sql_path = Path(script_file)
        if not sql_path.exists():
            raise FileNotFoundError(sql_path)
        raw_sql = sql_path.read_text()
        templated = substitute_variables(raw_sql, variables)
        for idx, statement in enumerate(split_queries(templated), start=1):
            start_time = time.perf_counter()
            message = f"Executing {sql_path} [statement {idx}]"
            (log or print)(message)
            execute_query(con, normalize_query(statement))
            duration = time.perf_counter() - start_time
            if log:
                log(f"Finished {sql_path} [statement {idx}] in {duration:.2f}s")
            if progress_cb:
                progress_cb()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run SQL scripts against DuckDB")
    parser.add_argument("scripts", nargs="+", help="SQL files to execute")
    parser.add_argument("-e", "--etlconf", dest="etlconf", help="Global config json")
    parser.add_argument("-c", "--config", dest="config", help="Workflow-level config json")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    etlconf = load_json(args.etlconf)
    workflow_conf = load_json(args.config)
    config = merge_config(DEFAULT_CONFIG, merge_config(etlconf, workflow_conf))
    run_scripts(args.scripts, config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
