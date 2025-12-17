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

## Run on PhysioNet demo data (end-to-end)

Assumes demo CSVs exist under `data/physionet.org/files/mimic-iv-demo/2.2/`:

```bash
python3 scripts/run_workflow.py -e conf/dev.etlconf
```

### Vocabulary selection

By default the runner uses:
- `data/vocab.duckdb` if present, otherwise
- `data/mock_vocab.duckdb`

Override explicitly:

```bash
python3 scripts/run_workflow.py -e conf/dev.etlconf --set @vocab_db_path=/path/to/vocab.duckdb
```

If you have a minimal vocab DB missing tables, bootstrap the empty table schemas from a full vocab DB:

```bash
python3 scripts/bootstrap_vocab_smoke.py --smoke-db data/mock_vocab.duckdb --full-db data/vocab.duckdb
```

## Outputs

- `omop_cdm.*`: internal ETL tables (includes `voc_*` views and custom vocab tables)
- `omop.*`: published OMOP tables (views, no `cdm_` prefix)
- `audit.*`: post-ETL audits (`audit.table_population`, `audit.mapping_rate`)

Quick sanity check with the DuckDB CLI:

```bash
duckdb data/mimiciv_demo.duckdb -c "select count(*) from omop.person;"
```

## Notes on IDs / types

- OMOP-facing numeric fields are emitted as `INTEGER` (matching OMOP CDM expectations).
- `person_id` is `subject_id`.
- `visit_occurrence_id` uses `hadm_id` when present; synthetic “no-hadm” visits use negative IDs.
