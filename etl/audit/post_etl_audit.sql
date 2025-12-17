-- -------------------------------------------------------------------
-- Post-ETL audits:
--  1) Population checks: if a source table has rows, the expected OMOP table
--     should also have rows (even if concept mapping is imperfect).
--  2) Mapping rates: % of rows mapped to standard concepts per OMOP table/field.
-- -------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS @audit_schema;

-- -------------------------------------------------------------------
-- Run metadata (persisted for traceability between runs)
-- -------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS @audit_schema.run_metadata (
    run_id VARCHAR,
    run_started_at TIMESTAMP,
    git_sha VARCHAR,
    duckdb_path VARCHAR,
    vocab_db_path VARCHAR,
    mimic_hosp_dir VARCHAR,
    mimic_icu_dir VARCHAR,
    mimic_derived_dir VARCHAR,
    mimic_waveform_dir VARCHAR,
    inserted_at TIMESTAMP DEFAULT now()
);

INSERT INTO @audit_schema.run_metadata (
    run_id,
    run_started_at,
    git_sha,
    duckdb_path,
    vocab_db_path,
    mimic_hosp_dir,
    mimic_icu_dir,
    mimic_derived_dir,
    mimic_waveform_dir,
    inserted_at
)
SELECT
    CAST('@run_id' AS VARCHAR) AS run_id,
    CAST('@run_started_at' AS TIMESTAMP) AS run_started_at,
    CAST('@git_sha' AS VARCHAR) AS git_sha,
    CAST('@duckdb_path' AS VARCHAR) AS duckdb_path,
    CAST('@vocab_db_path' AS VARCHAR) AS vocab_db_path,
    CAST('@mimic_hosp_dir' AS VARCHAR) AS mimic_hosp_dir,
    CAST('@mimic_icu_dir' AS VARCHAR) AS mimic_icu_dir,
    CAST('@mimic_derived_dir' AS VARCHAR) AS mimic_derived_dir,
    CAST('@mimic_waveform_dir' AS VARCHAR) AS mimic_waveform_dir,
    now() AS inserted_at
WHERE NOT EXISTS (
    SELECT 1 FROM @audit_schema.run_metadata WHERE run_id = CAST('@run_id' AS VARCHAR)
);

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
        '@etl_dataset.lk_services_clean' AS source_table,
        (SELECT COUNT(*) FROM @etl_project.@etl_dataset.lk_services_clean) AS source_rows,
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
-- 3) Unmapped source values (top offenders)
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @audit_schema.unmapped_top AS
WITH unioned AS (
    SELECT
        'condition_occurrence' AS table_name,
        'condition_concept_id' AS concept_field,
        condition_source_value AS source_value,
        COUNT(*)::BIGINT AS unmapped_rows
    FROM @omop_schema.condition_occurrence
    WHERE condition_concept_id = 0
    GROUP BY 1,2,3
    UNION ALL
    SELECT
        'procedure_occurrence' AS table_name,
        'procedure_concept_id' AS concept_field,
        procedure_source_value AS source_value,
        COUNT(*)::BIGINT AS unmapped_rows
    FROM @omop_schema.procedure_occurrence
    WHERE procedure_concept_id = 0
    GROUP BY 1,2,3
    UNION ALL
    SELECT
        'drug_exposure' AS table_name,
        'drug_concept_id' AS concept_field,
        drug_source_value AS source_value,
        COUNT(*)::BIGINT AS unmapped_rows
    FROM @omop_schema.drug_exposure
    WHERE drug_concept_id = 0
    GROUP BY 1,2,3
    UNION ALL
    SELECT
        'device_exposure' AS table_name,
        'device_concept_id' AS concept_field,
        device_source_value AS source_value,
        COUNT(*)::BIGINT AS unmapped_rows
    FROM @omop_schema.device_exposure
    WHERE device_concept_id = 0
    GROUP BY 1,2,3
    UNION ALL
    SELECT
        'measurement' AS table_name,
        'measurement_concept_id' AS concept_field,
        measurement_source_value AS source_value,
        COUNT(*)::BIGINT AS unmapped_rows
    FROM @omop_schema.measurement
    WHERE measurement_concept_id = 0
    GROUP BY 1,2,3
    UNION ALL
    SELECT
        'observation' AS table_name,
        'observation_concept_id' AS concept_field,
        observation_source_value AS source_value,
        COUNT(*)::BIGINT AS unmapped_rows
    FROM @omop_schema.observation
    WHERE observation_concept_id = 0
    GROUP BY 1,2,3
    UNION ALL
    SELECT
        'specimen' AS table_name,
        'specimen_concept_id' AS concept_field,
        specimen_source_value AS source_value,
        COUNT(*)::BIGINT AS unmapped_rows
    FROM @omop_schema.specimen
    WHERE specimen_concept_id = 0
    GROUP BY 1,2,3
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY table_name, concept_field ORDER BY unmapped_rows DESC, source_value) AS rn
    FROM unioned
)
SELECT table_name, concept_field, source_value, unmapped_rows
FROM ranked
WHERE rn <= 100
ORDER BY table_name, concept_field, unmapped_rows DESC, source_value
;

-- -------------------------------------------------------------------
-- 4) OMOP schema validation + basic DQ checks
-- -------------------------------------------------------------------

CREATE OR REPLACE TABLE @audit_schema.omop_schema_validation AS
WITH required_tables AS (
    SELECT * FROM (VALUES
        ('care_site'),
        ('condition_era'),
        ('condition_occurrence'),
        ('death'),
        ('device_exposure'),
        ('dose_era'),
        ('drug_era'),
        ('drug_exposure'),
        ('fact_relationship'),
        ('location'),
        ('measurement'),
        ('observation'),
        ('observation_period'),
        ('person'),
        ('procedure_occurrence'),
        ('source'),
        ('specimen'),
        ('visit_detail'),
        ('visit_occurrence')
    ) AS t(table_name)
),
existing AS (
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = '@omop_schema'
),
missing AS (
    SELECT rt.table_name
    FROM required_tables rt
    LEFT JOIN existing e USING (table_name)
    WHERE e.table_name IS NULL
)
SELECT
    'table_exists' AS check_name,
    table_name,
    CASE WHEN table_name IS NULL THEN 'OK' ELSE 'FAIL' END AS status,
    CASE WHEN table_name IS NULL THEN NULL ELSE CONCAT('Missing table: @omop_schema.', table_name) END AS details
FROM (SELECT table_name FROM missing UNION ALL SELECT NULL AS table_name LIMIT 1)
WHERE table_name IS NOT NULL
;

-- Basic PK/FK/NULL checks (lightweight, intended for smoke + quick sanity).
CREATE OR REPLACE TABLE @audit_schema.dq_checks AS
WITH checks AS (
    SELECT
        'pk_unique' AS check_type,
        'person' AS table_name,
        'person_id' AS field_name,
        (SELECT COUNT(*) FROM (SELECT person_id FROM @omop_schema.person GROUP BY person_id HAVING COUNT(*) > 1))::BIGINT AS fail_count
    UNION ALL
    SELECT
        'pk_unique', 'visit_occurrence', 'visit_occurrence_id',
        (SELECT COUNT(*) FROM (SELECT visit_occurrence_id FROM @omop_schema.visit_occurrence GROUP BY visit_occurrence_id HAVING COUNT(*) > 1))::BIGINT
    UNION ALL
    SELECT
        'pk_unique', 'condition_occurrence', 'condition_occurrence_id',
        (SELECT COUNT(*) FROM (SELECT condition_occurrence_id FROM @omop_schema.condition_occurrence GROUP BY condition_occurrence_id HAVING COUNT(*) > 1))::BIGINT
    UNION ALL
    SELECT
        'pk_unique', 'procedure_occurrence', 'procedure_occurrence_id',
        (SELECT COUNT(*) FROM (SELECT procedure_occurrence_id FROM @omop_schema.procedure_occurrence GROUP BY procedure_occurrence_id HAVING COUNT(*) > 1))::BIGINT
    UNION ALL
    SELECT
        'pk_unique', 'drug_exposure', 'drug_exposure_id',
        (SELECT COUNT(*) FROM (SELECT drug_exposure_id FROM @omop_schema.drug_exposure GROUP BY drug_exposure_id HAVING COUNT(*) > 1))::BIGINT
    UNION ALL
    SELECT
        'fk_person', 'visit_occurrence', 'person_id',
        (SELECT COUNT(*) FROM @omop_schema.visit_occurrence vo LEFT JOIN @omop_schema.person p ON p.person_id = vo.person_id WHERE p.person_id IS NULL)::BIGINT
    UNION ALL
    SELECT
        'fk_person', 'condition_occurrence', 'person_id',
        (SELECT COUNT(*) FROM @omop_schema.condition_occurrence co LEFT JOIN @omop_schema.person p ON p.person_id = co.person_id WHERE p.person_id IS NULL)::BIGINT
    UNION ALL
    SELECT
        'fk_visit', 'condition_occurrence', 'visit_occurrence_id',
        (SELECT COUNT(*) FROM @omop_schema.condition_occurrence co LEFT JOIN @omop_schema.visit_occurrence vo ON vo.visit_occurrence_id = co.visit_occurrence_id WHERE co.visit_occurrence_id IS NOT NULL AND vo.visit_occurrence_id IS NULL)::BIGINT
    UNION ALL
    SELECT
        'not_null', 'person', 'gender_concept_id',
        (SELECT COUNT(*) FROM @omop_schema.person WHERE gender_concept_id IS NULL)::BIGINT
)
SELECT
    check_type,
    table_name,
    field_name,
    fail_count,
    CASE WHEN fail_count > 0 THEN 'FAIL' ELSE 'OK' END AS status
FROM checks
ORDER BY check_type, table_name, field_name
;

-- -------------------------------------------------------------------
-- Fail the workflow if population checks show empty required OMOP tables.
-- -------------------------------------------------------------------

SELECT
    CASE
        WHEN EXISTS (SELECT 1 FROM @audit_schema.table_population WHERE status = 'FAIL')
            THEN CAST('' AS INT64)
        WHEN EXISTS (SELECT 1 FROM @audit_schema.omop_schema_validation WHERE status = 'FAIL')
            THEN CAST('' AS INT64)
        WHEN CAST('@audit_min_percent_mapped' AS DOUBLE) > 0
             AND EXISTS (
                 SELECT 1
                 FROM @audit_schema.mapping_rate
                 WHERE total_rows > 0
                   AND (
                       TRIM('@audit_mapping_tables') = ''
                       OR table_name IN (
                           SELECT TRIM(tn)
                           FROM UNNEST(string_split('@audit_mapping_tables', ',')) AS t(tn)
                           WHERE TRIM(tn) <> ''
                       )
                   )
                   AND percent_mapped < CAST('@audit_min_percent_mapped' AS DOUBLE)
             )
            THEN CAST('' AS INT64)
        WHEN CAST('@audit_min_percent_standard' AS DOUBLE) > 0
             AND EXISTS (
                 SELECT 1
                 FROM @audit_schema.mapping_rate
                 WHERE total_rows > 0
                   AND (
                       TRIM('@audit_mapping_tables') = ''
                       OR table_name IN (
                           SELECT TRIM(tn)
                           FROM UNNEST(string_split('@audit_mapping_tables', ',')) AS t(tn)
                           WHERE TRIM(tn) <> ''
                       )
                   )
                   AND percent_standard < CAST('@audit_min_percent_standard' AS DOUBLE)
             )
            THEN CAST('' AS INT64)
        WHEN CAST('@audit_fail_on_dq' AS INTEGER) = 1
             AND EXISTS (SELECT 1 FROM @audit_schema.dq_checks WHERE status = 'FAIL')
            THEN CAST('' AS INT64)
        ELSE 1
    END AS audit_ok
;
