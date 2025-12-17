# MIMIC-IV → OMOP CDM v5.4 (DuckDB)

This project converts **MIMIC-IV** (including PhysioNet **mimic-iv-demo/2.2**) into **OMOP CDM v5.4** using a DuckDB-first pipeline.

Demo dataset DOI: `https://doi.org/10.13026/p1f5-7x35`

## Install

```bash
# Preferred (venv)
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r requirements.txt

# If your system Python is "externally managed" (PEP 668), use:
python3 -m pip install --break-system-packages -r requirements.txt
```

## Quickstart (mock end-to-end)

Creates tiny mock MIMIC-IV CSVs and a minimal vocabulary DuckDB, then runs the full pipeline:

```bash
python3 test/mock_data/run_mock_pipeline.py
```

Note: `conf/mock.etlconf` is configured to **fail** if mapping rates for key OMOP tables are 0% (see “Audits and thresholds”).

Regenerate the committed mock inputs (useful after changing the mock generator):

```bash
python3 test/mock_data/generate_mock_data.py --base-dir data/mock_mimic --vocab-db data/mock_vocab.duckdb
```

## Run on PhysioNet demo data (end-to-end)

Assumes demo CSVs exist under `data/physionet.org/files/mimic-iv-demo/2.2/`:

```bash
python3 scripts/run_workflow.py -e conf/dev.etlconf
```

For a clean rerun (archive the prior DuckDB output first):

```bash
python3 scripts/clean_run.py --path data/mimiciv_demo.duckdb
python3 scripts/run_workflow.py -e conf/dev.etlconf
```

To skip loading crosswalk review CSVs (optional), point `@crosswalk_dir` to a non-existent folder:

```bash
python3 scripts/run_workflow.py -e conf/dev.etlconf --set @crosswalk_dir=/tmp/none
```

### Vocabulary selection

By default the runner uses:
- `data/vocab.duckdb` if present, otherwise
- `data/mock_vocab.duckdb`

`data/mock_vocab.duckdb` is only intended for smoke tests; for meaningful mapping rates you need a full Athena export in `data/vocab.duckdb`.

Override explicitly:

```bash
python3 scripts/run_workflow.py -e conf/dev.etlconf --set @vocab_db_path=/path/to/vocab.duckdb
```

Verify a vocab DuckDB (tables + columns), optionally with rowcount thresholds:

```bash
python3 scripts/verify_vocab_duckdb.py --database data/vocab.duckdb --schema main --min-concept-rows 1
```

If your vocab tables are under the legacy `vocab` schema (not `main`), override:

```bash
python3 scripts/run_workflow.py -e conf/dev.etlconf --set @vocab_schema=vocab --set @voc_dataset=vocab
```

If you have a minimal vocab DB missing tables, bootstrap the empty table schemas from a full vocab DB:

```bash
python3 scripts/bootstrap_vocab_smoke.py --smoke-db data/mock_vocab.duckdb --full-db data/vocab.duckdb
```

Create a tiny vocab snapshot (useful for CI/smoke tests) from a full vocab DB:

```bash
python3 scripts/create_vocab_snapshot.py --full-db data/vocab.duckdb --out-db data/vocab_snapshot.duckdb --schema main --concept-id 8527
```

## Outputs

- `omop_cdm.*`: internal ETL tables (includes `voc_*` views and custom vocab tables)
- `omop.*`: published OMOP tables (views, no `cdm_` prefix)
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
python3 scripts/run_workflow.py -e conf/dev.etlconf \
  --set @audit_min_percent_mapped=50 \
  --set @audit_min_percent_standard=50 \
  --set @audit_mapping_tables=condition_occurrence,procedure_occurrence,drug_exposure
```

### Optional: materialize OMOP tables

By default `omop.*` are views. To materialize them into base tables:

```bash
python3 scripts/run_workflow.py -e conf/dev.etlconf --set @publish_materialize=1
```

## Notes on IDs / types

- OMOP-facing numeric fields are emitted as `INTEGER` (matching OMOP CDM expectations).
- `person_id` is `subject_id`.
- `visit_occurrence_id` uses `hadm_id` when present; synthetic “no-hadm” visits use negative IDs.

## Mapping precedence / unknowns

- Base vocab comes from the attached vocab DuckDB (`@vocab_db_path`), exposed as `omop_cdm.voc_*` views.
- Custom mapping CSVs (`custom_mapping_csv/gcpt_*.csv`) are loaded into `omop_cdm.voc_custom_*` and UNION’ed into `omop_cdm.voc_*`.
- When a row cannot be mapped to a valid standard concept in the expected domain, the OMOP `*_concept_id` is set to `0` (kept for completeness; see `audit.mapping_rate` / `audit.unmapped_top`).

## Tests

```bash
python3 -m unittest discover -s test/py -p 'test_*.py'
```
