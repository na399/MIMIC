-- -------------------------------------------------------------------
-- Publish final OMOP CDM tables (clean names, separate schema)
-- -------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS @omop_schema;

-- Create views instead of duplicating data; this keeps the internal
-- @etl_dataset.cdm_* tables as the source of truth while exposing standard
-- OMOP table names (no cdm_ prefix) and hiding ETL metadata columns.

CREATE OR REPLACE VIEW @omop_schema.location AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_location;

CREATE OR REPLACE VIEW @omop_schema.care_site AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_care_site;

CREATE OR REPLACE VIEW @omop_schema.person AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_person;

CREATE OR REPLACE VIEW @omop_schema.death AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_death;

CREATE OR REPLACE VIEW @omop_schema.visit_occurrence AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_visit_occurrence;

CREATE OR REPLACE VIEW @omop_schema.visit_detail AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_visit_detail;

CREATE OR REPLACE VIEW @omop_schema.condition_occurrence AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_condition_occurrence;

CREATE OR REPLACE VIEW @omop_schema.procedure_occurrence AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_procedure_occurrence;

CREATE OR REPLACE VIEW @omop_schema.specimen AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_specimen;

CREATE OR REPLACE VIEW @omop_schema.measurement AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_measurement;

CREATE OR REPLACE VIEW @omop_schema.drug_exposure AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_drug_exposure;

CREATE OR REPLACE VIEW @omop_schema.device_exposure AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_device_exposure;

CREATE OR REPLACE VIEW @omop_schema.observation AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_observation;

CREATE OR REPLACE VIEW @omop_schema.observation_period AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_observation_period;

CREATE OR REPLACE VIEW @omop_schema.fact_relationship AS
SELECT * EXCLUDE (unit_id)
FROM @etl_project.@etl_dataset.cdm_fact_relationship;

CREATE OR REPLACE VIEW @omop_schema.condition_era AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id)
FROM @etl_project.@etl_dataset.cdm_condition_era;

CREATE OR REPLACE VIEW @omop_schema.drug_era AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id)
FROM @etl_project.@etl_dataset.cdm_drug_era;

CREATE OR REPLACE VIEW @omop_schema.dose_era AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id)
FROM @etl_project.@etl_dataset.cdm_dose_era;

-- Internal name is cdm_cdm_source; publish as OMOP-standard cdm_source.
CREATE OR REPLACE VIEW @omop_schema.cdm_source AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_cdm_source;
