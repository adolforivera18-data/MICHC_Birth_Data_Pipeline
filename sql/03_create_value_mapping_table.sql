
-- ============================================================
-- MICHC BIRTH DATA PIPELINE
-- Value Mapping Reference Table Creation
-- ============================================================
-- Database: michc_db_reference
-- Table: ref_michc_value_mapping
--
-- Purpose:
-- Create a reference table that translates original MICHC
-- categorical codes into descriptive labels for standardized
-- maternal and infant health analysis.
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS michc_db_reference.ref_michc_value_mapping (
    variable_name STRING,
    original_code STRING,
    descriptive_label STRING
)

-- Configure the CSV serialization and deserialization.

ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)

-- Define the storage format.

STORED AS TEXTFILE

-- Specify the MICHC value mapping reference data location.

LOCATION
    's3://publichealth-project-data-lake-152125350281-us-east-2-an/reference/ny-state/michc/'

-- Exclude the CSV header row.

TBLPROPERTIES (
    'skip.header.line.count' = '1'
);
