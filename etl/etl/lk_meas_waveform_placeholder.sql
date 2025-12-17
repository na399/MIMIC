-- Placeholder to satisfy workflows when waveform mapping is omitted.
CREATE OR REPLACE TABLE @etl_project.@etl_dataset.lk_meas_waveform_mapped (
    measurement_id INTEGER,
    subject_id INTEGER,
    hadm_id INTEGER,
    reference_id VARCHAR,
    target_concept_id INTEGER,
    target_domain_id VARCHAR,
    start_datetime TIMESTAMP,
    value_as_number DOUBLE,
    unit_concept_id INTEGER,
    source_code VARCHAR,
    source_concept_id INTEGER,
    unit_source_value VARCHAR,
    unit_id VARCHAR,
    load_table_id VARCHAR,
    load_row_id BIGINT,
    trace_id VARCHAR
);
