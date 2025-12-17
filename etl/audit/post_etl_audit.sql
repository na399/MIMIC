-- -------------------------------------------------------------------
-- Post-ETL audits:
--  1) Population checks: if a source table has rows, the expected OMOP table
--     should also have rows (even if concept mapping is imperfect).
--  2) Mapping rates: % of rows mapped to standard concepts per OMOP table/field.
-- -------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS @audit_schema;

-- -------------------------------------------------------------------
-- 1) Population checks
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @audit_schema.table_population AS
WITH checks AS (
    SELECT
        '@etl_dataset.src_patients' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_patients) AS source_rows,
        '@omop_schema.person' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.person) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_admissions' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_admissions) AS source_rows,
        '@omop_schema.visit_occurrence' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.visit_occurrence) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_patients' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_patients) AS source_rows,
        '@omop_schema.observation_period' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.observation_period) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_transfers' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_transfers) AS source_rows,
        '@omop_schema.visit_detail' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.visit_detail) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_admissions (deathtime not null)' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_admissions WHERE deathtime IS NOT NULL) AS source_rows,
        '@omop_schema.death' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.death) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_diagnoses_icd' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_diagnoses_icd) AS source_rows,
        '@omop_schema.condition_occurrence' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.condition_occurrence) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_procedures_icd' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_procedures_icd) AS source_rows,
        '@omop_schema.procedure_occurrence' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.procedure_occurrence) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_hcpcsevents' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_hcpcsevents) AS source_rows,
        '@omop_schema.procedure_occurrence' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.procedure_occurrence) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_procedureevents' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_procedureevents) AS source_rows,
        '@omop_schema.procedure_occurrence' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.procedure_occurrence) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_datetimeevents' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_datetimeevents) AS source_rows,
        '@omop_schema.procedure_occurrence' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.procedure_occurrence) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_prescriptions' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_prescriptions) AS source_rows,
        '@omop_schema.drug_exposure' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.drug_exposure) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.lk_chartevents_mapped + lk_drug_mapped (Device domain)' AS source_table,
        (
            (SELECT COUNT(*) FROM @etl_project.@etl_dataset.lk_chartevents_mapped WHERE target_domain_id = 'Device')
            + (SELECT COUNT(*) FROM @etl_project.@etl_dataset.lk_drug_mapped WHERE target_domain_id = 'Device')
        ) AS source_rows,
        '@omop_schema.device_exposure' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.device_exposure) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_labevents' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_labevents) AS source_rows,
        '@omop_schema.measurement' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.measurement) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_chartevents' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_chartevents) AS source_rows,
        '@omop_schema.measurement' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.measurement) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_outputevents' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_outputevents) AS source_rows,
        '@omop_schema.measurement' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.measurement) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_microbiologyevents' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_microbiologyevents) AS source_rows,
        '@omop_schema.specimen' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.specimen) AS target_rows
    UNION ALL
    SELECT
        '@etl_dataset.src_drgcodes' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.src_drgcodes) AS source_rows,
        '@omop_schema.observation' AS target_table,
        (SELECT COUNT(*) FROM @omop_schema.observation) AS target_rows
)
SELECT
    source_table,
    source_rows,
    target_table,
    target_rows,
    CASE
        WHEN source_rows = 0 THEN 'SKIP'
        WHEN target_rows = 0 THEN 'FAIL'
        ELSE 'OK'
    END AS status
FROM checks
;

-- -------------------------------------------------------------------
-- 2) Mapping rate audit (standard concept coverage)
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @audit_schema.mapping_rate AS
WITH rates AS (
    SELECT
        'condition_occurrence' AS table_name,
        'condition_concept_id' AS concept_field,
        COUNT(*)::BIGINT AS total_rows,
        COALESCE(SUM(CASE WHEN co.condition_concept_id != 0 THEN 1 ELSE 0 END), 0)::BIGINT AS mapped_rows,
        COALESCE(SUM(CASE WHEN vc.concept_id IS NOT NULL AND vc.standard_concept = 'S' AND vc.invalid_reason IS NULL THEN 1 ELSE 0 END), 0)::BIGINT AS standard_rows
    FROM @omop_schema.condition_occurrence co
    LEFT JOIN @etl_project.@etl_dataset.voc_concept vc
        ON vc.concept_id = co.condition_concept_id
    UNION ALL
    SELECT
        'procedure_occurrence' AS table_name,
        'procedure_concept_id' AS concept_field,
        COUNT(*)::BIGINT AS total_rows,
        COALESCE(SUM(CASE WHEN po.procedure_concept_id != 0 THEN 1 ELSE 0 END), 0)::BIGINT AS mapped_rows,
        COALESCE(SUM(CASE WHEN vc.concept_id IS NOT NULL AND vc.standard_concept = 'S' AND vc.invalid_reason IS NULL THEN 1 ELSE 0 END), 0)::BIGINT AS standard_rows
    FROM @omop_schema.procedure_occurrence po
    LEFT JOIN @etl_project.@etl_dataset.voc_concept vc
        ON vc.concept_id = po.procedure_concept_id
    UNION ALL
    SELECT
        'drug_exposure' AS table_name,
        'drug_concept_id' AS concept_field,
        COUNT(*)::BIGINT AS total_rows,
        COALESCE(SUM(CASE WHEN de.drug_concept_id != 0 THEN 1 ELSE 0 END), 0)::BIGINT AS mapped_rows,
        COALESCE(SUM(CASE WHEN vc.concept_id IS NOT NULL AND vc.standard_concept = 'S' AND vc.invalid_reason IS NULL THEN 1 ELSE 0 END), 0)::BIGINT AS standard_rows
    FROM @omop_schema.drug_exposure de
    LEFT JOIN @etl_project.@etl_dataset.voc_concept vc
        ON vc.concept_id = de.drug_concept_id
    UNION ALL
    SELECT
        'device_exposure' AS table_name,
        'device_concept_id' AS concept_field,
        COUNT(*)::BIGINT AS total_rows,
        COALESCE(SUM(CASE WHEN dx.device_concept_id != 0 THEN 1 ELSE 0 END), 0)::BIGINT AS mapped_rows,
        COALESCE(SUM(CASE WHEN vc.concept_id IS NOT NULL AND vc.standard_concept = 'S' AND vc.invalid_reason IS NULL THEN 1 ELSE 0 END), 0)::BIGINT AS standard_rows
    FROM @omop_schema.device_exposure dx
    LEFT JOIN @etl_project.@etl_dataset.voc_concept vc
        ON vc.concept_id = dx.device_concept_id
    UNION ALL
    SELECT
        'measurement' AS table_name,
        'measurement_concept_id' AS concept_field,
        COUNT(*)::BIGINT AS total_rows,
        COALESCE(SUM(CASE WHEN m.measurement_concept_id != 0 THEN 1 ELSE 0 END), 0)::BIGINT AS mapped_rows,
        COALESCE(SUM(CASE WHEN vc.concept_id IS NOT NULL AND vc.standard_concept = 'S' AND vc.invalid_reason IS NULL THEN 1 ELSE 0 END), 0)::BIGINT AS standard_rows
    FROM @omop_schema.measurement m
    LEFT JOIN @etl_project.@etl_dataset.voc_concept vc
        ON vc.concept_id = m.measurement_concept_id
    UNION ALL
    SELECT
        'observation' AS table_name,
        'observation_concept_id' AS concept_field,
        COUNT(*)::BIGINT AS total_rows,
        COALESCE(SUM(CASE WHEN o.observation_concept_id != 0 THEN 1 ELSE 0 END), 0)::BIGINT AS mapped_rows,
        COALESCE(SUM(CASE WHEN vc.concept_id IS NOT NULL AND vc.standard_concept = 'S' AND vc.invalid_reason IS NULL THEN 1 ELSE 0 END), 0)::BIGINT AS standard_rows
    FROM @omop_schema.observation o
    LEFT JOIN @etl_project.@etl_dataset.voc_concept vc
        ON vc.concept_id = o.observation_concept_id
    UNION ALL
    SELECT
        'specimen' AS table_name,
        'specimen_concept_id' AS concept_field,
        COUNT(*)::BIGINT AS total_rows,
        COALESCE(SUM(CASE WHEN s.specimen_concept_id != 0 THEN 1 ELSE 0 END), 0)::BIGINT AS mapped_rows,
        COALESCE(SUM(CASE WHEN vc.concept_id IS NOT NULL AND vc.standard_concept = 'S' AND vc.invalid_reason IS NULL THEN 1 ELSE 0 END), 0)::BIGINT AS standard_rows
    FROM @omop_schema.specimen s
    LEFT JOIN @etl_project.@etl_dataset.voc_concept vc
        ON vc.concept_id = s.specimen_concept_id
)
SELECT
    table_name,
    concept_field,
    total_rows,
    mapped_rows,
    ROUND(100.0 * mapped_rows / NULLIF(total_rows, 0), 2) AS percent_mapped,
    standard_rows,
    ROUND(100.0 * standard_rows / NULLIF(total_rows, 0), 2) AS percent_standard
FROM rates
ORDER BY table_name, concept_field
;

-- -------------------------------------------------------------------
-- Fail the workflow if population checks show empty required OMOP tables.
-- -------------------------------------------------------------------

SELECT
    CASE
        WHEN EXISTS (SELECT 1 FROM @audit_schema.table_population WHERE status = 'FAIL')
            THEN CAST('' AS INT64)
        ELSE 1
    END AS audit_ok
;
