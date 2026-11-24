
-- ============================================================
-- MICHC BIRTH DATA PIPELINE
-- Indicator Mapping Reference Table Creation
-- ============================================================
-- Database: michc_db_reference
-- Table: ref_michc_indicator_mapping
--
-- Purpose:
-- Create a reference table that defines maternal and infant
-- health indicator identifiers, descriptive titles, indicator
-- groups, and display order.
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS michc_db_reference.ref_michc_indicator_mapping (
    indicator_id STRING,
    indicator_title STRING,
    indicator_group STRING,
    sort_order STRING
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

-- Specify the MICHC indicator mapping reference data location.

LOCATION
    's3://publichealth-project-data-lake-152125350281-us-east-2-an/reference/ny-state/michc/indicator-mapping/'

-- Exclude the CSV header row.

TBLPROPERTIES (
    'skip.header.line.count' = '1'
);
