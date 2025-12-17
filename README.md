# MIMIC-IV → OMOP CDM v5.4 (DuckDB)

This project converts **MIMIC-IV** (including PhysioNet **mimic-iv-demo/2.2**) into **OMOP CDM v5.4** using a DuckDB-first pipeline.

Demo dataset DOI: `https://doi.org/10.13026/p1f5-7x35`

## Install

```bash
# Preferred (venv)
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

## Run full ETL pipeline

```bash
uv run python scripts/run_workflow.py -e conf/full.etlconf
```

## Quickstart (mock end-to-end)

Creates tiny mock MIMIC-IV CSVs and a minimal vocabulary DuckDB, then runs the full pipeline:

```bash
uv run python test/mock_data/run_mock_pipeline.py
```

Note: `conf/mock.etlconf` is configured to **fail** if mapping rates for key OMOP tables are 0% (see “Audits and thresholds”).

Regenerate the committed mock inputs (useful after changing the mock generator):

```bash
uv run python test/mock_data/generate_mock_data.py --base-dir data/mock_mimic --vocab-db data/mock_vocab.duckdb
```

## Run on PhysioNet demo data (end-to-end)

Assumes demo CSVs exist under `data/physionet.org/files/mimic-iv-demo/2.2/`:

```bash
uv run python scripts/run_workflow.py -e conf/dev.etlconf
```

## Run on full MIMIC-IV v3.1 (smoke/sample)

If you have PhysioNet MIMIC-IV v3.1 extracted at `/home/natthawut/mimic/physionet.org/files/mimiciv/3.1`, this runs a fast sample ETL by ingesting only the tables used by the ETL and limiting each table to the first N rows:

```bash
uv run python scripts/clean_run.py --path data/mimiciv31_smoke.duckdb
uv run python scripts/run_workflow.py -e conf/full.etlconf \
  --set @duckdb_path=data/mimiciv31_smoke.duckdb \
  --set @mimic_hosp_dir=../mimic/physionet.org/files/mimiciv/3.1/hosp \
  --set @mimic_icu_dir=../mimic/physionet.org/files/mimiciv/3.1/icu \
  --set @mimic_derived_dir= \
  --set @ingest_row_limit=20000
```

Notes:
- MIMIC-IV v2+ stores `patients`, `admissions`, and `transfers` in the `hosp/` folder.
- `conf/full.etlconf` and `conf/dev.etlconf` default to ingesting only the MIMIC tables used by this ETL; to ingest everything, set `--set @ingest_include_tables=` (empty).
- For a full load, omit `@ingest_row_limit` (expect a much longer runtime).

For a clean rerun (archive the prior DuckDB output first):

```bash
uv run python scripts/clean_run.py --path data/mimiciv_demo.duckdb
uv run python scripts/run_workflow.py -e conf/dev.etlconf
```

To skip loading crosswalk review CSVs (optional), point `@crosswalk_dir` to a non-existent folder:

```bash
uv run python scripts/run_workflow.py -e conf/dev.etlconf --set @crosswalk_dir=/tmp/none
```

### Vocabulary selection

By default the runner uses:
- `data/vocab.duckdb` if present, otherwise
- `data/mock_vocab.duckdb`

`data/mock_vocab.duckdb` is only intended for smoke tests; for meaningful mapping rates you need a full Athena export in `data/vocab.duckdb`.

Override explicitly:

```bash
uv run python scripts/run_workflow.py -e conf/dev.etlconf --set @vocab_db_path=/path/to/vocab.duckdb
```

Verify a vocab DuckDB (tables + columns), optionally with rowcount thresholds:

```bash
uv run python scripts/verify_vocab_duckdb.py --database data/vocab.duckdb --schema main --min-concept-rows 1
```

If your vocab tables are under the legacy `vocab` schema (not `main`), override:

```bash
uv run python scripts/run_workflow.py -e conf/dev.etlconf --set @vocab_schema=vocab --set @voc_dataset=vocab
```

If you have a minimal vocab DB missing tables, bootstrap the empty table schemas from a full vocab DB:

```bash
uv run python scripts/bootstrap_vocab_smoke.py --smoke-db data/mock_vocab.duckdb --full-db data/vocab.duckdb
```

Create a tiny vocab snapshot (useful for CI/smoke tests) from a full vocab DB:

```bash
uv run python scripts/create_vocab_snapshot.py --full-db data/vocab.duckdb --out-db data/vocab_snapshot.duckdb --schema main --concept-id 8527
```

## Outputs

- `omop_cdm.*`: internal ETL tables (includes `voc_*` views and custom vocab tables)
- `omop.*`: published OMOP tables (views, no `cdm_` prefix; `omop.source` corresponds to OMOP `cdm_source`)
- `audit.*`: post-ETL audits (`audit.table_population`, `audit.mapping_rate`)
- Optional: `omop_cdm.crosswalk_d_items_to_concept` loaded from `crosswalk_csv/d_items_to_concept.csv` (mapping review aid).

Quick sanity check with the DuckDB CLI:

```bash
duckdb data/mimiciv_demo.duckdb -c "select count(*) from omop.person;"
```

### Audits and thresholds

The workflow writes audit tables into `audit.*` and can fail the run based on:
- population checks (`audit.table_population`)
- schema checks (`audit.omop_schema_validation`, `audit.dq_checks`)
- mapping thresholds (`audit.mapping_rate`)

Key knobs:
- `@audit_min_percent_mapped` / `@audit_min_percent_standard` (0 disables)
- `@audit_mapping_tables` (comma-separated list of `audit.mapping_rate.table_name` values to enforce; empty means “all”)
- `@audit_fail_on_dq` (set to `1` to fail on `audit.dq_checks` FAIL rows)

Example: enforce mapping rates only for key domains while leaving others informational:

```bash
uv run python scripts/run_workflow.py -e conf/dev.etlconf \
  --set @audit_min_percent_mapped=50 \
  --set @audit_min_percent_standard=50 \
  --set @audit_mapping_tables=condition_occurrence,procedure_occurrence,drug_exposure
```

### Optional: materialize OMOP tables

By default `omop.*` are views. To materialize them into base tables:

```bash
uv run python scripts/run_workflow.py -e conf/dev.etlconf --set @publish_materialize=1
```

## Performance tuning (full v3.x)

The pipeline defaults are conservative. For full MIMIC-IV v3.x runs, try:

- Increase DuckDB resources (see `scripts/duckdb_run_script.py` settings in your config): `threads`, `memory_limit`, `temp_directory`.
- Use the optional post-staging index workflow (included in `conf/full.etlconf`, disabled by default):

```bash
uv run python scripts/run_workflow.py -e conf/full.etlconf \
  --set @optimize_enable=1
```

By default this targets smaller “join key” tables (e.g., `src_admissions`, `src_diagnoses_icd`). Indexing the largest fact-like staging tables can be expensive; enable it only if profiling shows a win:

```bash
uv run python scripts/run_workflow.py -e conf/full.etlconf \
  --set @optimize_enable=1 \
  --set @optimize_enable_large=1
```

The curated index list lives in `scripts/optimize_duckdb_indexes.py` (`INDEX_SPECS`).

## Notes on IDs / types

- OMOP-facing numeric fields are emitted as `INTEGER` (matching OMOP CDM expectations).
- `person_id` is `subject_id`.
- `visit_occurrence_id` uses `hadm_id` when present; synthetic “no-hadm” visits use negative IDs.

## Ingest DDL (official schema)

`scripts/ingest_mimic_csv_to_duckdb.py` uses the official MIMIC-IV CREATE TABLE definitions in `etl/mimic_ddl/mimiciv_hosp_icu_create_tables.sql` by default (no CSV type inference). For troubleshooting, you can fall back to inference with `--infer-schema`.

## Mapping precedence / unknowns

- Base vocab comes from the attached vocab DuckDB (`@vocab_db_path`), exposed as `omop_cdm.voc_*` views.
- Custom mapping CSVs (`custom_mapping_csv/gcpt_*.csv`) are loaded into `omop_cdm.voc_custom_*` and UNION’ed into `omop_cdm.voc_*`.
- When a row cannot be mapped to a valid standard concept in the expected domain, the OMOP `*_concept_id` is set to `0` (kept for completeness; see `audit.mapping_rate` / `audit.unmapped_top`).

## Tests

```bash
uv run python -m unittest discover -s test/py -p 'test_*.py'
```
