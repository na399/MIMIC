-- -------------------------------------------------------------------
-- @2020, Odysseus Data Services, Inc. All rights reserved
-- MIMIC IV CDM Conversion
-- -------------------------------------------------------------------

-- -------------------------------------------------------------------
-- Copy vocabulary tables from the master vocab dataset
-- (to apply custom mapping here?)
-- -------------------------------------------------------------------

-- check
-- SELECT 'VOC', COUNT(*) FROM `@voc_project`.@voc_dataset.concept
-- UNION ALL
-- SELECT 'TARGET', COUNT(*) FROM @etl_project.@etl_dataset.voc_concept
-- ;

-- Use views backed by the attached vocab DuckDB to avoid copying millions of
-- rows into the ETL database. Custom mappings are loaded into *_custom_* tables
-- and UNION'ed into the public voc_* views.

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_custom_concept AS
SELECT * FROM `@voc_project`.@voc_dataset.concept LIMIT 0
;

CREATE OR REPLACE TABLE @etl_project.@etl_dataset.voc_custom_concept_relationship AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_relationship LIMIT 0
;

CREATE OR REPLACE VIEW @etl_project.@etl_dataset.voc_concept AS
SELECT * FROM `@voc_project`.@voc_dataset.concept
UNION ALL
SELECT * FROM @etl_project.@etl_dataset.voc_custom_concept
;

CREATE OR REPLACE VIEW @etl_project.@etl_dataset.voc_concept_relationship AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_relationship
UNION ALL
SELECT * FROM @etl_project.@etl_dataset.voc_custom_concept_relationship
;

CREATE OR REPLACE VIEW @etl_project.@etl_dataset.voc_vocabulary AS
SELECT * FROM `@voc_project`.@voc_dataset.vocabulary
;

CREATE OR REPLACE VIEW @etl_project.@etl_dataset.voc_domain AS
SELECT * FROM `@voc_project`.@voc_dataset.domain
;
CREATE OR REPLACE VIEW @etl_project.@etl_dataset.voc_concept_class AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_class
;
CREATE OR REPLACE VIEW @etl_project.@etl_dataset.voc_relationship AS
SELECT * FROM `@voc_project`.@voc_dataset.relationship
;
CREATE OR REPLACE VIEW @etl_project.@etl_dataset.voc_concept_synonym AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_synonym
;
CREATE OR REPLACE VIEW @etl_project.@etl_dataset.voc_concept_ancestor AS
SELECT * FROM `@voc_project`.@voc_dataset.concept_ancestor
;
CREATE OR REPLACE VIEW @etl_project.@etl_dataset.voc_drug_strength AS
SELECT
    TRY_CAST(NULLIF(CAST(drug_concept_id AS STRING), '') AS INT64)             AS drug_concept_id,
    TRY_CAST(NULLIF(CAST(ingredient_concept_id AS STRING), '') AS INT64)       AS ingredient_concept_id,
    TRY_CAST(NULLIF(CAST(amount_value AS STRING), '') AS FLOAT64)              AS amount_value,
    TRY_CAST(NULLIF(CAST(amount_unit_concept_id AS STRING), '') AS INT64)      AS amount_unit_concept_id,
    TRY_CAST(NULLIF(CAST(numerator_value AS STRING), '') AS FLOAT64)           AS numerator_value,
    TRY_CAST(NULLIF(CAST(numerator_unit_concept_id AS STRING), '') AS INT64)   AS numerator_unit_concept_id,
    TRY_CAST(NULLIF(CAST(denominator_value AS STRING), '') AS FLOAT64)         AS denominator_value,
    TRY_CAST(NULLIF(CAST(denominator_unit_concept_id AS STRING), '') AS INT64) AS denominator_unit_concept_id,
    TRY_CAST(NULLIF(CAST(box_size AS STRING), '') AS FLOAT64)                  AS box_size,
    TRY_CAST(NULLIF(CAST(valid_start_date AS STRING), '') AS DATE)             AS valid_start_date,
    TRY_CAST(NULLIF(CAST(valid_end_date AS STRING), '') AS DATE)               AS valid_end_date,
    NULLIF(CAST(invalid_reason AS STRING), '')                                 AS invalid_reason
FROM `@voc_project`.@voc_dataset.drug_strength
;
