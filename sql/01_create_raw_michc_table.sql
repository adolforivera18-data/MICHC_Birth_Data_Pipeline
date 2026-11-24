
-- ============================================================
-- MICHC BIRTH DATA PIPELINE
-- Raw Athena Table Creation
-- ============================================================
-- Database: michc_db_raw
-- Table: raw_michc_opencsv
--
-- Purpose:
-- Create an external Athena table for the preprocessed
-- MICHC birth records stored in Amazon S3.
-- ============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS michc_db_raw.raw_michc_opencsv (
    facility_of_birth_pfi STRING,
    county_of_birth STRING,
    zip_of_birth STRING,
    mother_residence_county STRING,
    mother_residence_zip STRING,
    mother_residence_state STRING,
    mailing_address_locality STRING,
    infant_sex STRING,
    birthweight_grams STRING,
    mother_race_white STRING,
    mother_race_black STRING,
    asian_indian STRING,
    chinese STRING,
    filipino STRING,
    japanese STRING,
    korean STRING,
    vietnamese STRING,
    native_hawaiian STRING,
    guamanian_or_chamarro STRING,
    samoan STRING,
    american_indian_or_alaska_native STRING,
    other_asian STRING,
    other_pacific_islander STRING,
    other_race STRING,
    mother_not_hispanic STRING,
    mexican STRING,
    puerto_rican STRING,
    cuban STRING,
    other_hispanic STRING,
    mother_education_level STRING,
    mother_employed_during_pregnancy STRING,
    prenatal_care_received STRING,
    estimated_due_date STRING,
    date_first_prenatal_visit STRING,
    date_last_prenatal_visit STRING,
    number_prenatal_visits STRING,
    clinical_estimate_gestation_weeks STRING,
    attendant_license_number STRING,
    infant_alive_at_report_time STRING,
    syphilis_test_given STRING,
    date_syphilis_test STRING,
    reason_no_syphilis_test STRING,
    newborn_treatment_given STRING,
    infant_feeding_after_discharge STRING,
    nicu_admission STRING,
    hepatitis_b_immunization STRING,
    route_and_method STRING,
    maternal_medical_risk_none STRING,
    other_serious_chronic_diseases STRING,
    abruptio_placenta STRING,
    diabetes_pre_pregnancy STRING,
    diabetes_gestational STRING,
    hypertension_pre_pregnancy STRING,
    hypertension_pregnancy_related STRING,
    eclampsia STRING,
    previous_preterm_infant STRING,
    poor_pregnancy_outcomes STRING,
    vaginal_bleeding STRING,
    infertility_treatment STRING,
    fertility_enhancing_drugs STRING,
    assisted_reproductive_technology STRING,
    prelabor_referral_high_risk STRING,
    previous_cesarean_section STRING,
    number_previous_cesarean_sections STRING,
    infections_none STRING,
    gonorrhea STRING,
    syphilis STRING,
    genital_herpes STRING,
    chlamydia STRING,
    hepatitis_b STRING,
    hepatitis_c STRING,
    tuberculosis STRING,
    congenital_malformations_none STRING,
    anencephaly STRING,
    meningomyelocele_spina_bifida STRING,
    cyanotic_congenital_heart_disease STRING,
    congenital_diaphragmatic_hernia STRING,
    omphalocele STRING,
    gastroschisis STRING,
    limb_reduction_defect STRING,
    cleft_lip_palate STRING,
    cleft_palate_alone STRING,
    down_syndrome STRING,
    down_syndrome_karyotype_confirmed STRING,
    down_syndrome_karyotype_pending STRING,
    suspected_chromosomal_disorder STRING,
    suspected_chromosomal_disorder_karyotype_confirmed STRING,
    suspected_chromosomal_disorder_karyotype_pending STRING,
    hypospadias STRING,
    congenital_malformations_unknown STRING,
    labor_delivery_characteristics_none STRING,
    induction_labor_arom STRING,
    induction_labor_medical STRING,
    augmentation_labor STRING,
    maternal_morbidity_none STRING,
    maternal_transfusion STRING,
    perineal_laceration STRING,
    ruptured_uterus STRING,
    unplanned_hysterectomy STRING,
    admit_to_icu STRING,
    unplanned_operation STRING,
    maternal_morbidity_unknown STRING,
    tobacco_use STRING,
    alcohol_use STRING,
    illegal_drug_use STRING,
    maternal_weight_pre_pregnancy STRING,
    maternal_weight_at_delivery STRING,
    maternal_height STRING,
    primary_payor STRING,
    medicaid_secondary_payor STRING,
    participation_in_wic STRING,
    maternal_age STRING
)

-- Define the S3 partition structure.

PARTITIONED BY (
    county_name STRING,
    birth_year STRING,
    extract_year STRING,
    extract_month STRING
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

-- Specify the raw MICHC data location.

LOCATION
    's3://publichealth-project-data-lake-152125350281-us-east-2-an/raw/ny-state/michc/'

-- Exclude the CSV header row.

TBLPROPERTIES (
    'skip.header.line.count' = '1'
);
