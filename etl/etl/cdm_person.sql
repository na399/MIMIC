-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Populate cdm_person table
-- 
-- Dependencies: run after st_hosp_base.sql
-- on Demo: 12.4 sec
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Known issues / Open points:
--
-- TRUNCATE TABLE is not supported, organize "create or replace"
-- @etl_project.@etl_dataset.cdm_person;
--
-- negative unique id from FARM_FINGERPRINT(GENERATE_UUID())
--
-- loaded custom mapping: 
--      gcpt_ethnicity_to_concept -> mimiciv_per_ethnicity
--
-- Why don't we want to use subject_id as person_id and hadm_id as visit_occurrence_id?
--      ask analysts
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- tmp_subject_ethnicity
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.tmp_subject_ethnicity AS
SELECT DISTINCT
    src.subject_id                      AS subject_id,
    FIRST_VALUE(src.ethnicity) OVER (
        PARTITION BY src.subject_id 
        ORDER BY src.admittime ASC)     AS ethnicity_first
FROM
    @etl_project.@etl_dataset.src_admissions src
;

-- -------------------------------------------------------------------
-- lk_pat_ethnicity_concept
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_pat_ethnicity_concept AS
WITH candidates AS (
    SELECT
        src.ethnicity_first     AS source_code,
        vc.concept_id           AS source_concept_id,
        vc.vocabulary_id        AS source_vocabulary_id,
        vc1.concept_id          AS target_concept_id,
        vc1.vocabulary_id       AS target_vocabulary_id -- distinguish Race vs Ethnicity
    FROM
        @etl_project.@etl_dataset.tmp_subject_ethnicity src
    LEFT JOIN
        -- gcpt_ethnicity_to_concept -> mimiciv_per_ethnicity
        @etl_project.@etl_dataset.voc_concept vc
            ON UPPER(vc.concept_code) = UPPER(src.ethnicity_first)
            AND vc.domain_id IN ('Race', 'Ethnicity')
    LEFT JOIN
        @etl_project.@etl_dataset.voc_concept_relationship cr1
            ON  cr1.concept_id_1 = vc.concept_id
            AND cr1.relationship_id = 'Maps to'
    LEFT JOIN
        @etl_project.@etl_dataset.voc_concept vc1
            ON  cr1.concept_id_2 = vc1.concept_id
            AND vc1.invalid_reason IS NULL
            AND vc1.standard_concept = 'S'
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY source_code
            ORDER BY
                CASE WHEN target_concept_id IS NOT NULL THEN 1 ELSE 0 END DESC,
                CASE WHEN source_vocabulary_id LIKE 'mimiciv%' THEN 1 ELSE 0 END DESC,
                source_concept_id DESC
        ) AS rn
    FROM candidates
)
SELECT
    source_code,
    source_concept_id,
    source_vocabulary_id,
    target_concept_id,
    target_vocabulary_id
FROM ranked
WHERE rn = 1
;

-- -------------------------------------------------------------------
-- cdm_person
-- -------------------------------------------------------------------

--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.cdm_person
(
    person_id                   INTEGER   not null ,
    gender_concept_id           INTEGER   not null ,
    year_of_birth               INTEGER   not null ,
    month_of_birth              INTEGER            ,
    day_of_birth                INTEGER            ,
    birth_datetime              TIMESTAMP          ,
    race_concept_id             INTEGER   not null,
    ethnicity_concept_id        INTEGER   not null,
    location_id                 INTEGER            ,
    provider_id                 INTEGER            ,
    care_site_id                INTEGER            ,
    person_source_value         STRING             ,
    gender_source_value         STRING             ,
    gender_source_concept_id    INTEGER            ,
    race_source_value           STRING             ,
    race_source_concept_id      INTEGER            ,
    ethnicity_source_value      STRING             ,
    ethnicity_source_concept_id INTEGER            ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   BIGINT,
    trace_id                      STRING
)
;

INSERT INTO @etl_project.@etl_dataset.cdm_person
SELECT
    CAST(p.subject_id AS INTEGER)     AS person_id,
    CASE 
        WHEN p.gender = 'F' THEN 8532 -- FEMALE
        WHEN p.gender = 'M' THEN 8507 -- MALE
        ELSE 0 
    END                             AS gender_concept_id,
    p.anchor_year-p.anchor_age      AS year_of_birth,
    CAST(NULL AS INTEGER)           AS month_of_birth,
    CAST(NULL AS INTEGER)           AS day_of_birth,
    CAST(NULL AS TIMESTAMP)         AS birth_datetime,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
                THEN map_eth.target_concept_id
            ELSE NULL
        END, 0)                               AS race_concept_id,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id = 'Ethnicity'
                THEN map_eth.target_concept_id
            ELSE NULL
        END, 0)                     AS ethnicity_concept_id,
    CAST(NULL AS INTEGER)           AS location_id,
    CAST(NULL AS INTEGER)           AS provider_id,
    CAST(NULL AS INTEGER)           AS care_site_id,
    CAST(p.subject_id AS STRING)    AS person_source_value,
    p.gender                        AS gender_source_value,
    0                               AS gender_source_concept_id,
    CASE
        WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
            THEN eth.ethnicity_first
        ELSE NULL
    END                             AS race_source_value,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
                THEN map_eth.source_concept_id
            ELSE NULL
        END, 0)                        AS race_source_concept_id,
    CASE
        WHEN map_eth.target_vocabulary_id = 'Ethnicity'
            THEN eth.ethnicity_first
        ELSE NULL
    END                             AS ethnicity_source_value,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id = 'Ethnicity'
                THEN map_eth.source_concept_id
            ELSE NULL
        END, 0)                     AS ethnicity_source_concept_id,
    -- 
    'person.patients'               AS unit_id,
    p.load_table_id                 AS load_table_id,
    p.load_row_id                   AS load_row_id,
    p.trace_id                      AS trace_id
FROM 
    @etl_project.@etl_dataset.src_patients p
LEFT JOIN 
    @etl_project.@etl_dataset.tmp_subject_ethnicity eth 
        ON  p.subject_id = eth.subject_id
LEFT JOIN
    @etl_project.@etl_dataset.lk_pat_ethnicity_concept map_eth
        ON  eth.ethnicity_first = map_eth.source_code
;


-- -------------------------------------------------------------------
-- cleanup
-- -------------------------------------------------------------------

DROP TABLE IF EXISTS @etl_project.@etl_dataset.tmp_subject_ethnicity;
