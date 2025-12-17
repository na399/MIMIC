-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate staging tables for patient + admission + transfer data.
--
-- MIMIC-IV v2+ stores patients/admissions/transfers in the hosp module, so
-- this script always reads from @hosp_dataset.
--
-- Dependencies: run first after DDL (before other staging scripts).
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- src_patients
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_patients AS
SELECT
    subject_id                          AS subject_id,
    anchor_year                         AS anchor_year,
    anchor_age                          AS anchor_age,
    anchor_year_group                   AS anchor_year_group,
    gender                              AS gender,
    --
    'patients'                          AS load_table_id,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
        subject_id AS subject_id
    )))                                 AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.patients
;

-- -------------------------------------------------------------------
-- src_admissions
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_admissions AS
SELECT
    hadm_id                             AS hadm_id, -- PK
    subject_id                          AS subject_id,
    admittime                           AS admittime,
    dischtime                           AS dischtime,
    deathtime                           AS deathtime,
    admission_type                      AS admission_type,
    admission_location                  AS admission_location,
    discharge_location                  AS discharge_location,
    race                                AS ethnicity, -- MIMIC IV 2.0 change, field race replaced field ethnicity
    edregtime                           AS edregtime,
    insurance                           AS insurance,
    marital_status                      AS marital_status,
    language                            AS language,
    -- edouttime
    -- hospital_expire_flag
    --
    'admissions'                        AS load_table_id,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        hadm_id AS hadm_id
    )))                                 AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        hadm_id AS hadm_id
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.admissions
;

-- -------------------------------------------------------------------
-- src_transfers
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.src_transfers AS
SELECT
    transfer_id                         AS transfer_id,
    hadm_id                             AS hadm_id,
    subject_id                          AS subject_id,
    careunit                            AS careunit,
    intime                              AS intime,
    outtime                             AS outtime,
    eventtype                           AS eventtype,
    --
    'transfers'                         AS load_table_id,
    FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        hadm_id AS hadm_id,
        transfer_id AS transfer_id
    )))                                 AS load_row_id,
    TO_JSON_STRING(STRUCT(
        subject_id AS subject_id,
        hadm_id AS hadm_id,
        transfer_id AS transfer_id
    ))                                  AS trace_id
FROM
    @source_project.@hosp_dataset.transfers
;
