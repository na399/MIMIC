-- -------------------------------------------------------------------
-- Initialize ID sequences (INT-safe) for DuckDB runs.
-- These sequences are used to avoid 64-bit FARM_FINGERPRINT UUID ids in OMOP tables.
-- -------------------------------------------------------------------

DROP SEQUENCE IF EXISTS @etl_dataset.seq_condition_occurrence_id;
CREATE SEQUENCE @etl_dataset.seq_condition_occurrence_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_procedure_occurrence_id;
CREATE SEQUENCE @etl_dataset.seq_procedure_occurrence_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_drug_exposure_id;
CREATE SEQUENCE @etl_dataset.seq_drug_exposure_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_device_exposure_id;
CREATE SEQUENCE @etl_dataset.seq_device_exposure_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_measurement_id;
CREATE SEQUENCE @etl_dataset.seq_measurement_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_specimen_id;
CREATE SEQUENCE @etl_dataset.seq_specimen_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_observation_id;
CREATE SEQUENCE @etl_dataset.seq_observation_id START 1;

-- Synthetic IDs for visits without natural keys (we use negative values in lk_vis_part_2.sql).
DROP SEQUENCE IF EXISTS @etl_dataset.seq_synthetic_visit_occurrence_id;
CREATE SEQUENCE @etl_dataset.seq_synthetic_visit_occurrence_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_synthetic_visit_detail_id;
CREATE SEQUENCE @etl_dataset.seq_synthetic_visit_detail_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_condition_era_id;
CREATE SEQUENCE @etl_dataset.seq_condition_era_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_drug_era_id;
CREATE SEQUENCE @etl_dataset.seq_drug_era_id START 1;

DROP SEQUENCE IF EXISTS @etl_dataset.seq_dose_era_id;
CREATE SEQUENCE @etl_dataset.seq_dose_era_id START 1;
