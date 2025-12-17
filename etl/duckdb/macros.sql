-- DuckDB compatibility macros for legacy BigQuery-oriented SQL
CREATE SCHEMA IF NOT EXISTS util;

-- hashing/uuid helpers
CREATE OR REPLACE MACRO FARM_FINGERPRINT(val) AS abs(hash(val));

-- date/time helpers
CREATE OR REPLACE MACRO PARSE_DATE(fmt, val) AS CAST(val AS DATE);
CREATE OR REPLACE MACRO DATETIME(date_part, time_part) AS CAST(date_part || ' ' || coalesce(time_part, '00:00:00') AS TIMESTAMP);
CREATE OR REPLACE MACRO CURRENT_DATETIME() AS current_timestamp;

-- JSON helpers
CREATE OR REPLACE MACRO TO_JSON_STRING(val) AS to_json(val);

-- DuckDB already supports STRUCT_PACK/TO_JSON, but a lightweight macro gives a
-- consistent entry point when migrating from STRUCT(...)/TO_JSON_STRING(...).
CREATE OR REPLACE MACRO STRUCT_PACK_FROM_JSON(val) AS to_json(val);

-- BigQuery REGEXP_EXTRACT returns NULL when there's no match, while DuckDB's
-- regexp_extract returns an empty string. We expose a separate macro and
-- rewrite calls in the runner to avoid shadowing DuckDB's built-in function.
CREATE OR REPLACE MACRO BQ_REGEXP_EXTRACT(val, pattern) AS nullif(regexp_extract(val, pattern), '');
