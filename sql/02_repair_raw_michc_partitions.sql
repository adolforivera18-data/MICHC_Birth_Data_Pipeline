-- ============================================================
-- MICHC BIRTH DATA PIPELINE
-- Raw Table Partition Repair
-- ============================================================
-- Database: michc_db_raw
-- Table: raw_michc_opencsv
--
-- Purpose:
-- Discover and register new S3 partitions in Athena
-- following the upload of annual or monthly MICHC extracts.
-- ============================================================

MSCK REPAIR TABLE michc_db_raw.raw_michc_opencsv;
