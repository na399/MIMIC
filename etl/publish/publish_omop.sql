-- -------------------------------------------------------------------
-- Publish final OMOP CDM tables as base tables (no views).
--
-- Goals:
--  - Keep the final output in a single schema (@omop_schema; default: main)
--  - Remove the internal cdm_ prefix from table names
--  - Materialize (tables) so the output is self-contained
--  - Copy OMOP vocabulary tables into the destination DuckDB
-- -------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS @omop_schema;

-- -------------------------
-- Clinical (CDM) tables
-- -------------------------

DROP VIEW IF EXISTS @omop_schema.location;
DROP TABLE IF EXISTS @omop_schema.location;
CREATE TABLE @omop_schema.location AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_location;

DROP VIEW IF EXISTS @omop_schema.care_site;
DROP TABLE IF EXISTS @omop_schema.care_site;
CREATE TABLE @omop_schema.care_site AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_care_site;

DROP VIEW IF EXISTS @omop_schema.person;
DROP TABLE IF EXISTS @omop_schema.person;
CREATE TABLE @omop_schema.person AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_person;

DROP VIEW IF EXISTS @omop_schema.death;
DROP TABLE IF EXISTS @omop_schema.death;
CREATE TABLE @omop_schema.death AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_death;

DROP VIEW IF EXISTS @omop_schema.visit_occurrence;
DROP TABLE IF EXISTS @omop_schema.visit_occurrence;
CREATE TABLE @omop_schema.visit_occurrence AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_visit_occurrence;

DROP VIEW IF EXISTS @omop_schema.visit_detail;
DROP TABLE IF EXISTS @omop_schema.visit_detail;
CREATE TABLE @omop_schema.visit_detail AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_visit_detail;

DROP VIEW IF EXISTS @omop_schema.condition_occurrence;
DROP TABLE IF EXISTS @omop_schema.condition_occurrence;
CREATE TABLE @omop_schema.condition_occurrence AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_condition_occurrence;

DROP VIEW IF EXISTS @omop_schema.procedure_occurrence;
DROP TABLE IF EXISTS @omop_schema.procedure_occurrence;
CREATE TABLE @omop_schema.procedure_occurrence AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_procedure_occurrence;

DROP VIEW IF EXISTS @omop_schema.specimen;
DROP TABLE IF EXISTS @omop_schema.specimen;
CREATE TABLE @omop_schema.specimen AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_specimen;

DROP VIEW IF EXISTS @omop_schema.measurement;
DROP TABLE IF EXISTS @omop_schema.measurement;
CREATE TABLE @omop_schema.measurement AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_measurement;

DROP VIEW IF EXISTS @omop_schema.drug_exposure;
DROP TABLE IF EXISTS @omop_schema.drug_exposure;
CREATE TABLE @omop_schema.drug_exposure AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_drug_exposure;

DROP VIEW IF EXISTS @omop_schema.device_exposure;
DROP TABLE IF EXISTS @omop_schema.device_exposure;
CREATE TABLE @omop_schema.device_exposure AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_device_exposure;

DROP VIEW IF EXISTS @omop_schema.observation;
DROP TABLE IF EXISTS @omop_schema.observation;
CREATE TABLE @omop_schema.observation AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_observation;

DROP VIEW IF EXISTS @omop_schema.observation_period;
DROP TABLE IF EXISTS @omop_schema.observation_period;
CREATE TABLE @omop_schema.observation_period AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_observation_period;

DROP VIEW IF EXISTS @omop_schema.fact_relationship;
DROP TABLE IF EXISTS @omop_schema.fact_relationship;
CREATE TABLE @omop_schema.fact_relationship AS
SELECT * EXCLUDE (unit_id)
FROM @etl_project.@etl_dataset.cdm_fact_relationship;

DROP VIEW IF EXISTS @omop_schema.condition_era;
DROP TABLE IF EXISTS @omop_schema.condition_era;
CREATE TABLE @omop_schema.condition_era AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id)
FROM @etl_project.@etl_dataset.cdm_condition_era;

DROP VIEW IF EXISTS @omop_schema.drug_era;
DROP TABLE IF EXISTS @omop_schema.drug_era;
CREATE TABLE @omop_schema.drug_era AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id)
FROM @etl_project.@etl_dataset.cdm_drug_era;

DROP VIEW IF EXISTS @omop_schema.dose_era;
DROP TABLE IF EXISTS @omop_schema.dose_era;
CREATE TABLE @omop_schema.dose_era AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id)
FROM @etl_project.@etl_dataset.cdm_dose_era;

-- Internal name is cdm_cdm_source; publish without a cdm_ prefix for cleaner output.
DROP VIEW IF EXISTS @omop_schema.source;
DROP TABLE IF EXISTS @omop_schema.source;
CREATE TABLE @omop_schema.source AS
SELECT * EXCLUDE (unit_id, load_table_id, load_row_id, trace_id)
FROM @etl_project.@etl_dataset.cdm_cdm_source;

-- -------------------------
-- Vocabulary tables
-- -------------------------

DROP VIEW IF EXISTS @omop_schema.concept;
DROP TABLE IF EXISTS @omop_schema.concept;
CREATE TABLE @omop_schema.concept AS
SELECT * FROM @etl_project.@etl_dataset.voc_concept;

DROP VIEW IF EXISTS @omop_schema.concept_relationship;
DROP TABLE IF EXISTS @omop_schema.concept_relationship;
CREATE TABLE @omop_schema.concept_relationship AS
SELECT * FROM @etl_project.@etl_dataset.voc_concept_relationship;

DROP VIEW IF EXISTS @omop_schema.vocabulary;
DROP TABLE IF EXISTS @omop_schema.vocabulary;
CREATE TABLE @omop_schema.vocabulary AS
SELECT * FROM `@voc_project`.@voc_dataset.vocabulary;

DROP VIEW IF EXISTS @omop_schema.domain;
DROP TABLE IF EXISTS @omop_schema.domain;
CREATE TABLE @omop_schema.domain AS
SELECT * FROM `@voc_project`.@voc_dataset.domain;

DROP VIEW IF EXISTS @omop_schema.concept_class;
DROP TABLE IF EXISTS @omop_schema.concept_class;
CREATE TABLE @omop_schema.concept_class AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_class;

DROP VIEW IF EXISTS @omop_schema.relationship;
DROP TABLE IF EXISTS @omop_schema.relationship;
CREATE TABLE @omop_schema.relationship AS
SELECT * FROM `@voc_project`.@voc_dataset.relationship;

DROP VIEW IF EXISTS @omop_schema.concept_synonym;
DROP TABLE IF EXISTS @omop_schema.concept_synonym;
CREATE TABLE @omop_schema.concept_synonym AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_synonym;

DROP VIEW IF EXISTS @omop_schema.concept_ancestor;
DROP TABLE IF EXISTS @omop_schema.concept_ancestor;
CREATE TABLE @omop_schema.concept_ancestor AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_ancestor;

DROP VIEW IF EXISTS @omop_schema.drug_strength;
DROP TABLE IF EXISTS @omop_schema.drug_strength;
CREATE TABLE @omop_schema.drug_strength AS
SELECT * FROM `@voc_project`.@voc_dataset.drug_strength;
