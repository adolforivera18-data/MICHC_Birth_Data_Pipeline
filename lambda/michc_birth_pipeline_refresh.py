
# ============================================================
# MICHC BIRTH DATA PIPELINE
# AWS Lambda Automation
# ============================================================
# Purpose:
# Refresh the MICHC Birth Data Pipeline following the upload
# of new annual or monthly MICHC birth data extracts.
#
# Pipeline operations:
# 1. Repair raw Athena table partitions.
# 2. Drop and rebuild the curated birth records table.
# 3. Drop and rebuild the curated birth indicators table.
# 4. Initiate a QuickSight SPICE dataset refresh.
# ============================================================

import time
import uuid
import boto3


# ============================================================
# AWS CONFIGURATION
# ============================================================

REGION = "us-east-2"
ACCOUNT_ID = "YOUR_AWS_ACCOUNT_ID"

BUCKET = "YOUR_S3_BUCKET_NAME"

ATHENA_DATABASE = "michc_db_curated"
ATHENA_WORKGROUP = "primary"
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/michc/"

QUICKSIGHT_DATASET_ID = "YOUR_QUICKSIGHT_DATASET_ID"

BIRTH_RECORDS_PREFIX = "curated/ny-state/michc/birth_records/"
BIRTH_INDICATORS_PREFIX = "curated/ny-state/michc/birth_indicators/"


# ============================================================
# AWS SERVICE CLIENTS
# ============================================================

athena = boto3.client("athena", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
quicksight = boto3.client("quicksight", region_name=REGION)


# ============================================================
# ATHENA QUERY EXECUTION
# ============================================================

def run_athena_query(sql, database=ATHENA_DATABASE):
    """
    Execute an Athena SQL query and wait for completion.
    Stop the pipeline if the query fails.
    """

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP
    )

    query_id = response["QueryExecutionId"]

    print(f"Started Athena query: {query_id}")

    while True:

        result = athena.get_query_execution(
            QueryExecutionId=query_id
        )

        status = result["QueryExecution"]["Status"]
        state = status["State"]

        if state == "SUCCEEDED":

            print(f"Athena query succeeded: {query_id}")

            return query_id

        if state in ("FAILED", "CANCELLED"):

            reason = status.get(
                "StateChangeReason",
                "Unknown Athena error"
            )

            raise RuntimeError(
                f"Athena query {query_id} {state}: {reason}"
            )

        time.sleep(5)


# ============================================================
# S3 PREFIX DELETION
# ============================================================

def delete_s3_prefix(prefix):
    """
    Delete existing objects under a specified S3 prefix.
    """

    print(f"Deleting existing S3 data: {prefix}")

    paginator = s3.get_paginator("list_objects_v2")

    deleted_count = 0

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=prefix
    ):

        objects = [
            {"Key": obj["Key"]}
            for obj in page.get("Contents", [])
        ]

        if objects:

            response = s3.delete_objects(
                Bucket=BUCKET,
                Delete={"Objects": objects}
            )

            if response.get("Errors"):

                raise RuntimeError(
                    f"S3 deletion failed: {response['Errors']}"
                )

            deleted_count += len(objects)

    print(f"Deleted {deleted_count} S3 objects")


# ============================================================
# CURATED BIRTH RECORDS SQL QUERY
# ============================================================

# region BIRTH_RECORDS_SQL

BIRTH_RECORDS_SQL = rf"""

CREATE TABLE michc_db_curated.michc_birth_records

WITH (
    format = 'PARQUET',
    external_location = 's3://{BUCKET}/{BIRTH_RECORDS_PREFIX}',
    partitioned_by = ARRAY['geographic_area', 'birth_year']
)

AS

-- =====================================================
-- 1. IDENTIFY THE LATEST EXTRACT FOR EACH BIRTH YEAR
-- =====================================================

WITH latest_extract AS (

    SELECT
        county_name,
        birth_year,

        MAX(
            TRY_CAST(extract_year AS INTEGER) * 100
            + TRY_CAST(extract_month AS INTEGER)
        ) AS latest_extract_yyyymm

    FROM michc_db_raw.raw_michc_opencsv

    GROUP BY
        county_name,
        birth_year
),

-- =====================================================
-- 2. SELECT ELIGIBLE ORANGE COUNTY RESIDENT BIRTHS
-- =====================================================

eligible_births AS (

    SELECT
        r.*

    FROM michc_db_raw.raw_michc_opencsv r

    INNER JOIN latest_extract le

        ON r.county_name = le.county_name

        AND r.birth_year = le.birth_year

        AND (
            TRY_CAST(r.extract_year AS INTEGER) * 100
            + TRY_CAST(r.extract_month AS INTEGER)
        ) = le.latest_extract_yyyymm

    WHERE r.county_name = 'orange'
        AND TRIM(r.mother_residence_county) = '71'
),

-- =====================================================
-- 3. STANDARDIZE ORIGINAL BIRTH VARIABLES
-- =====================================================

standardized_births AS (

    SELECT

        county_name,
        extract_year,
        extract_month,

        -- Geography and facility

        TRIM(facility_of_birth_pfi)
            AS facility_of_birth_pfi,

        TRIM(county_of_birth)
            AS county_of_birth,

        TRIM(mother_residence_county)
            AS mother_residence_county,

        TRIM(mother_residence_zip)
            AS mother_residence_zip,

        COALESCE(
            NULLIF(
                REGEXP_REPLACE(
                    UPPER(TRIM(mailing_address_locality)),
                    '\s+',
                    ' '
                ),
                ''
            ),
            'UNKNOWN'
        ) AS locality,

        -- Maternal demographics

        TRY_CAST(TRIM(maternal_age) AS INTEGER)
            AS maternal_age,

        TRIM(mother_education_level)
            AS mother_education_level,

        TRIM(mother_employed_during_pregnancy)
            AS mother_employed_during_pregnancy,

        -- Hispanic ethnicity

        TRIM(mother_not_hispanic)
            AS mother_not_hispanic,

        TRIM(mexican) AS mexican,
        TRIM(puerto_rican) AS puerto_rican,
        TRIM(cuban) AS cuban,
        TRIM(other_hispanic) AS other_hispanic,

        -- Maternal race

        TRIM(mother_race_white) AS mother_race_white,
        TRIM(mother_race_black) AS mother_race_black,

        TRIM(asian_indian) AS asian_indian,
        TRIM(chinese) AS chinese,
        TRIM(filipino) AS filipino,
        TRIM(japanese) AS japanese,
        TRIM(korean) AS korean,
        TRIM(vietnamese) AS vietnamese,
        TRIM(other_asian) AS other_asian,

        TRIM(native_hawaiian) AS native_hawaiian,

        TRIM(guamanian_or_chamarro)
            AS guamanian_or_chamarro,

        TRIM(samoan) AS samoan,

        TRIM(other_pacific_islander)
            AS other_pacific_islander,

        TRIM(american_indian_or_alaska_native)
            AS american_indian_or_alaska_native,

        TRIM(other_race) AS other_race,

        -- Maternal risk factors

        TRIM(tobacco_use) AS tobacco_use,
        TRIM(alcohol_use) AS alcohol_use,
        TRIM(illegal_drug_use) AS illegal_drug_use,

        TRIM(diabetes_pre_pregnancy)
            AS diabetes_pre_pregnancy,

        TRIM(diabetes_gestational)
            AS diabetes_gestational,

        TRIM(hypertension_pre_pregnancy)
            AS hypertension_pre_pregnancy,

        TRIM(hypertension_pregnancy_related)
            AS hypertension_pregnancy_related,

        TRY_CAST(
            TRIM(maternal_weight_pre_pregnancy) AS DOUBLE
        ) AS maternal_weight_pre_pregnancy,

        TRY_CAST(
            TRIM(maternal_weight_at_delivery) AS DOUBLE
        ) AS maternal_weight_at_delivery,

        TRY_CAST(TRIM(maternal_height) AS DOUBLE)
            AS maternal_height,

        -- Prenatal care

        TRIM(prenatal_care_received)
            AS prenatal_care_received,

        TRIM(estimated_due_date)
            AS estimated_due_date,

        TRIM(date_first_prenatal_visit)
            AS date_first_prenatal_visit,

        TRIM(date_last_prenatal_visit)
            AS date_last_prenatal_visit,

        TRY_CAST(TRIM(number_prenatal_visits) AS INTEGER)
            AS number_prenatal_visits,

        -- Labor and delivery

        TRIM(route_and_method)
            AS route_and_method,

        TRIM(previous_cesarean_section)
            AS previous_cesarean_section,

        TRIM(induction_labor_arom)
            AS induction_labor_arom,

        TRIM(induction_labor_medical)
            AS induction_labor_medical,

        TRIM(augmentation_labor)
            AS augmentation_labor,

        -- Infant characteristics and health outcomes

        TRIM(infant_sex)
            AS infant_sex,

        TRY_CAST(TRIM(birthweight_grams) AS INTEGER)
            AS birthweight_grams,

        TRY_CAST(
            TRIM(clinical_estimate_gestation_weeks)
            AS INTEGER
        ) AS gestational_age_weeks,

        TRIM(infant_alive_at_report_time)
            AS infant_alive_at_report_time,

        TRIM(nicu_admission)
            AS nicu_admission,

        -- Infant preventive care and feeding

        TRIM(hepatitis_b_immunization)
            AS hepatitis_b_immunization,

        TRIM(infant_feeding_after_discharge)
            AS infant_feeding_after_discharge,

        TRIM(newborn_treatment_given)
            AS newborn_treatment_given,

        -- Insurance and assistance

        TRIM(primary_payor)
            AS primary_payor,

        TRIM(medicaid_secondary_payor)
            AS medicaid_secondary_payor,

        TRIM(participation_in_wic)
            AS participation_in_wic,

        -- Partition variables

        'Orange County' AS geographic_area,

        TRY_CAST(birth_year AS INTEGER)
            AS birth_year

    FROM eligible_births
),

-- =====================================================
-- 4. PREPARE CATEGORICAL REFERENCE MAPPINGS
-- =====================================================

value_mapping AS (

    SELECT

        TRIM(variable_name) AS variable_name,

        TRIM(original_code) AS original_code,

        TRIM(descriptive_label) AS descriptive_label

    FROM michc_db_reference.ref_michc_value_mapping
),

-- =====================================================
-- 5. APPLY CATEGORICAL REFERENCE MAPPINGS
-- =====================================================

recoded_births AS (

    SELECT

        s.*,

        -- Maternal demographics

        COALESCE(education.descriptive_label, 'Unknown')
            AS maternal_education_label,

        COALESCE(hispanic.descriptive_label, 'Unknown')
            AS hispanic_label,

        COALESCE(white.descriptive_label, 'Unknown')
            AS white_label,

        COALESCE(black.descriptive_label, 'Unknown')
            AS black_label,

        COALESCE(aian.descriptive_label, 'Unknown')
            AS american_indian_alaskan_native_label,

        COALESCE(other_race.descriptive_label, 'Unknown')
            AS other_race_label,

        -- Maternal risk factors

        COALESCE(alcohol.descriptive_label, 'Unknown')
            AS alcohol_risk_label,

        COALESCE(tobacco.descriptive_label, 'Unknown')
            AS tobacco_label,

        COALESCE(drug.descriptive_label, 'Unknown')
            AS drug_use_label,

        COALESCE(diabetes_pre.descriptive_label, 'Unknown')
            AS diabetes_pre_preg_label,

        COALESCE(diabetes_gest.descriptive_label, 'Unknown')
            AS gestation_diabetes_label,

        COALESCE(hypertension_pre.descriptive_label, 'Unknown')
            AS hypertension_pre_preg_label,

        COALESCE(hypertension_preg.descriptive_label, 'Unknown')
            AS hypertension_preg_related_label,

        -- Prenatal care

        COALESCE(prenatal.descriptive_label, 'Unknown')
            AS prenatal_care_received_label,

        -- Labor and delivery

        COALESCE(delivery.descriptive_label, 'Unknown')
            AS delivery_method_label,

        CASE

            WHEN COALESCE(s.facility_of_birth_pfi, '') = ''
                THEN 'Unknown'

            ELSE COALESCE(
                facility.descriptive_label,
                'Other'
            )

        END AS facility_label,

        -- Insurance

        COALESCE(payor.descriptive_label, 'Unknown')
            AS primary_payor_label,

        -- Infant preventive care and feeding

        COALESCE(feeding.descriptive_label, 'Unknown')
            AS infant_feeding_label,

        COALESCE(hep_b.descriptive_label, 'Unknown')
            AS hep_b_immunization_label

    FROM standardized_births s

    LEFT JOIN value_mapping education
        ON education.variable_name = 'maternal_education'
        AND education.original_code =
            s.mother_education_level

    LEFT JOIN value_mapping hispanic
        ON hispanic.variable_name = 'hispanic'
        AND hispanic.original_code =
            s.mother_not_hispanic

    LEFT JOIN value_mapping white
        ON white.variable_name = 'white'
        AND white.original_code =
            s.mother_race_white

    LEFT JOIN value_mapping black
        ON black.variable_name = 'black'
        AND black.original_code =
            s.mother_race_black

    LEFT JOIN value_mapping aian
        ON aian.variable_name = 'american_indian_alaskan_native'
        AND aian.original_code =
            s.american_indian_or_alaska_native

    LEFT JOIN value_mapping other_race
        ON other_race.variable_name = 'other_race'
        AND other_race.original_code =
            s.other_race

    LEFT JOIN value_mapping alcohol
        ON alcohol.variable_name = 'alcohol_risk'
        AND alcohol.original_code =
            s.alcohol_use

    LEFT JOIN value_mapping tobacco
        ON tobacco.variable_name = 'tobacco'
        AND tobacco.original_code =
            s.tobacco_use

    LEFT JOIN value_mapping drug
        ON drug.variable_name = 'drug_use'
        AND drug.original_code =
            s.illegal_drug_use

    LEFT JOIN value_mapping diabetes_pre
        ON diabetes_pre.variable_name = 'diabetes_pre_preg'
        AND diabetes_pre.original_code =
            s.diabetes_pre_pregnancy

    LEFT JOIN value_mapping diabetes_gest
        ON diabetes_gest.variable_name = 'gestation_diabetes'
        AND diabetes_gest.original_code =
            s.diabetes_gestational

    LEFT JOIN value_mapping hypertension_pre
        ON hypertension_pre.variable_name = 'hypertension_pre_preg'
        AND hypertension_pre.original_code =
            s.hypertension_pre_pregnancy

    LEFT JOIN value_mapping hypertension_preg
        ON hypertension_preg.variable_name = 'hypertension_preg_related'
        AND hypertension_preg.original_code =
            s.hypertension_pregnancy_related

    LEFT JOIN value_mapping prenatal
        ON prenatal.variable_name = 'prenatal_care_received'
        AND prenatal.original_code =
            s.prenatal_care_received

    LEFT JOIN value_mapping delivery
        ON delivery.variable_name = 'delivery_method'
        AND delivery.original_code =
            s.route_and_method

    LEFT JOIN value_mapping facility
        ON facility.variable_name = 'facility'
        AND facility.original_code =
            s.facility_of_birth_pfi

    LEFT JOIN value_mapping payor
        ON payor.variable_name = 'primary_payor'
        AND payor.original_code =
            s.primary_payor

    LEFT JOIN value_mapping feeding
        ON feeding.variable_name = 'infant_feeding'
        AND feeding.original_code =
            s.infant_feeding_after_discharge

    LEFT JOIN value_mapping hep_b
        ON hep_b.variable_name = 'hep_b_immunization'
        AND hep_b.original_code =
            s.hepatitis_b_immunization
),

-- =====================================================
-- 6. DERIVE CALCULATED BIRTH VARIABLES
-- =====================================================

calculated_births AS (

    SELECT

        s.*,

        -- Pre-pregnancy BMI

        CASE

            WHEN maternal_weight_pre_pregnancy > 0
                AND maternal_height > 0

            THEN
                703.0 * maternal_weight_pre_pregnancy
                / POWER(maternal_height, 2)

            ELSE NULL

        END AS maternal_bmi,

        -- Birthweight range

        CASE

            WHEN birthweight_grams IS NULL
                OR birthweight_grams <= 0
                THEN 'Unknown'

            WHEN birthweight_grams >= 2500
                THEN '2500+'

            WHEN birthweight_grams >= 1500
                THEN '1500-2499'

            ELSE '0-1499'

        END AS birthweight_range,

        -- Maternal age range

        CASE

            WHEN maternal_age IS NULL
                OR maternal_age NOT BETWEEN 10 AND 60
                THEN 'Unknown'

            WHEN maternal_age >= 55 THEN '55+'
            WHEN maternal_age >= 45 THEN '45-54'
            WHEN maternal_age >= 35 THEN '35-44'
            WHEN maternal_age >= 25 THEN '25-34'
            WHEN maternal_age >= 20 THEN '20-24'
            WHEN maternal_age >= 18 THEN '18-19'
            WHEN maternal_age >= 15 THEN '15-17'
            WHEN maternal_age >= 10 THEN '10-14'

            ELSE 'Unknown'

        END AS maternal_age_range,

        -- Gestational age range

        CASE

            WHEN gestational_age_weeks IS NULL
                OR gestational_age_weeks <= 0
                THEN 'Unknown'

            WHEN gestational_age_weeks >= 37
                THEN '>=37 Weeks'

            WHEN gestational_age_weeks >= 32
                THEN '32-36 Weeks'

            ELSE '<32 Weeks'

        END AS gestational_age_range,

        -- Prenatal care adequacy

        CASE

            WHEN number_prenatal_visits IS NULL
                OR number_prenatal_visits < 0
                THEN 'Unknown'

            WHEN number_prenatal_visits >= 12
                THEN 'Adequate'

            ELSE '<12'

        END AS prenatal_care_adequacy,

        -- Combined Asian race category

        CASE

            WHEN '1' IN (
                asian_indian,
                chinese,
                filipino,
                japanese,
                korean,
                vietnamese,
                other_asian
            )
                THEN 'Yes'

            WHEN asian_indian = '0'
                AND chinese = '0'
                AND filipino = '0'
                AND japanese = '0'
                AND korean = '0'
                AND vietnamese = '0'
                AND other_asian = '0'
                THEN 'No'

            ELSE 'Unknown'

        END AS asian_label,

        -- Combined Pacific Islander race category

        CASE

            WHEN '1' IN (
                native_hawaiian,
                guamanian_or_chamarro,
                samoan,
                other_pacific_islander
            )
                THEN 'Yes'

            WHEN native_hawaiian = '0'
                AND guamanian_or_chamarro = '0'
                AND samoan = '0'
                AND other_pacific_islander = '0'
                THEN 'No'

            ELSE 'Unknown'

        END AS pacific_islander_label

    FROM recoded_births s
),

-- =====================================================
-- 7. CLASSIFY CALCULATED BMI
-- =====================================================

classified_births AS (

    SELECT

        c.*,

        CASE

            WHEN maternal_bmi IS NULL
                OR maternal_bmi <= 0
                THEN 'Unknown'

            WHEN maternal_bmi >= 30
                THEN 'Obese'

            WHEN maternal_bmi >= 25
                THEN 'Overweight'

            WHEN maternal_bmi >= 18.5
                THEN 'Normal'

            ELSE 'Underweight'

        END AS maternal_bmi_range

    FROM calculated_births c
)

-- =====================================================
-- 8. FINAL CURATED BIRTH RECORDS
-- =====================================================

SELECT

    -- Extract metadata

    county_name,
    extract_year,
    extract_month,

    -- Geography and facility

    facility_of_birth_pfi,
    facility_label,
    county_of_birth,
    mother_residence_county,
    mother_residence_zip,
    locality,

    -- Maternal demographics

    maternal_age,
    maternal_age_range,
    mother_education_level,
    maternal_education_label,
    mother_employed_during_pregnancy,

    -- Hispanic ethnicity

    mother_not_hispanic,
    mexican,
    puerto_rican,
    cuban,
    other_hispanic,
    hispanic_label,

    -- Maternal race

    mother_race_white,
    white_label,

    mother_race_black,
    black_label,

    asian_indian,
    chinese,
    filipino,
    japanese,
    korean,
    vietnamese,
    other_asian,
    asian_label,

    native_hawaiian,
    guamanian_or_chamarro,
    samoan,
    other_pacific_islander,
    pacific_islander_label,

    american_indian_or_alaska_native,
    american_indian_alaskan_native_label,

    other_race,
    other_race_label,

    -- Maternal risk factors

    tobacco_use,
    tobacco_label,

    alcohol_use,
    alcohol_risk_label,

    illegal_drug_use,
    drug_use_label,

    diabetes_pre_pregnancy,
    diabetes_pre_preg_label,

    diabetes_gestational,
    gestation_diabetes_label,

    hypertension_pre_pregnancy,
    hypertension_pre_preg_label,

    hypertension_pregnancy_related,
    hypertension_preg_related_label,

    maternal_weight_pre_pregnancy,
    maternal_weight_at_delivery,
    maternal_height,
    maternal_bmi,
    maternal_bmi_range,

    -- Prenatal care

    prenatal_care_received,
    prenatal_care_received_label,
    estimated_due_date,
    date_first_prenatal_visit,
    date_last_prenatal_visit,
    number_prenatal_visits,
    prenatal_care_adequacy,

    -- Labor and delivery

    route_and_method,
    delivery_method_label,
    previous_cesarean_section,
    induction_labor_arom,
    induction_labor_medical,
    augmentation_labor,

    -- Infant characteristics and health outcomes

    infant_sex,
    birthweight_grams,
    birthweight_range,
    gestational_age_weeks,
    gestational_age_range,
    infant_alive_at_report_time,
    nicu_admission,

    -- Infant preventive care and feeding

    hepatitis_b_immunization,
    hep_b_immunization_label,
    infant_feeding_after_discharge,
    infant_feeding_label,
    newborn_treatment_given,

    -- Insurance and assistance

    primary_payor,
    primary_payor_label,
    medicaid_secondary_payor,
    participation_in_wic,

    -- Partition columns must remain last

    geographic_area,
    birth_year

FROM classified_births;

"""

# endregion


# ============================================================
# CURATED BIRTH INDICATORS SQL QUERY
# ============================================================

# region BIRTH_INDICATORS_SQL

BIRTH_INDICATORS_SQL = rf"""

CREATE TABLE michc_db_curated.michc_birth_indicators

WITH (
    format = 'PARQUET',
    external_location = 's3://{BUCKET}/{BIRTH_INDICATORS_PREFIX}',
    partitioned_by = ARRAY['geographic_area', 'birth_year']
)

AS

WITH geographic_births AS (

    -- Represent each birth at the county level
    -- and at its original locality level.

    SELECT
        geographic_area,
        birth_year,
        'County' AS geographic_level,
        'Orange County' AS locality,

        maternal_age_range,
        maternal_education_label,
        hispanic_label,
        white_label,
        black_label,
        asian_label,
        pacific_islander_label,
        american_indian_alaskan_native_label,
        other_race_label,
        primary_payor_label,

        tobacco_label,
        alcohol_risk_label,
        drug_use_label,
        diabetes_pre_preg_label,
        gestation_diabetes_label,
        hypertension_pre_preg_label,
        hypertension_preg_related_label,
        maternal_bmi_range,

        prenatal_care_received_label,
        prenatal_care_adequacy,

        delivery_method_label,
        facility_label,

        birthweight_range,
        birthweight_grams,
        gestational_age_range,
        gestational_age_weeks,

        nicu_admission,
        hep_b_immunization_label,
        infant_feeding_label

    FROM michc_db_curated.michc_birth_records

    UNION ALL

    SELECT
        geographic_area,
        birth_year,
        'Locality' AS geographic_level,
        locality,

        maternal_age_range,
        maternal_education_label,
        hispanic_label,
        white_label,
        black_label,
        asian_label,
        pacific_islander_label,
        american_indian_alaskan_native_label,
        other_race_label,
        primary_payor_label,

        tobacco_label,
        alcohol_risk_label,
        drug_use_label,
        diabetes_pre_preg_label,
        gestation_diabetes_label,
        hypertension_pre_preg_label,
        hypertension_preg_related_label,
        maternal_bmi_range,

        prenatal_care_received_label,
        prenatal_care_adequacy,

        delivery_method_label,
        facility_label,

        birthweight_range,
        birthweight_grams,
        gestational_age_range,
        gestational_age_weeks,

        nicu_admission,
        hep_b_immunization_label,
        infant_feeding_label

    FROM michc_db_curated.michc_birth_records
),

standardized_indicators AS (

    SELECT
        geographic_area,
        birth_year,
        geographic_level,
        locality,

        -- Maternal demographic breakdowns

        COALESCE(maternal_age_range, 'Unknown')
            AS maternal_age_group,

        COALESCE(hispanic_label, 'Unknown')
            AS maternal_ethnicity,

        COALESCE(white_label, 'Unknown')
            AS maternal_race_white,

        COALESCE(black_label, 'Unknown')
            AS maternal_race_black,

        COALESCE(asian_label, 'Unknown')
            AS maternal_race_asian,

        COALESCE(pacific_islander_label, 'Unknown')
            AS maternal_race_pacific_islander,

        COALESCE(
            american_indian_alaskan_native_label,
            'Unknown'
        ) AS maternal_race_aian,

        COALESCE(other_race_label, 'Unknown')
            AS maternal_race_other,

        indicator_id,

        COALESCE(
            NULLIF(TRIM(indicator_category), ''),
            'Unknown'
        ) AS indicator_category

    FROM geographic_births

    CROSS JOIN UNNEST(

        ARRAY[
            'maternal_age',
            'maternal_education',
            'maternal_ethnicity',
            'maternal_race_white',
            'maternal_race_black',
            'maternal_race_asian',
            'maternal_race_pacific_islander',
            'maternal_race_aian',
            'maternal_race_other',
            'primary_payor',

            'tobacco_use',
            'alcohol_use',
            'drug_use',
            'prepregnancy_diabetes',
            'gestational_diabetes',
            'prepregnancy_hypertension',
            'pregnancy_related_hypertension',
            'maternal_bmi',

            'prenatal_care_received',
            'prenatal_care_adequate',

            'delivery_method',
            'cesarean_delivery',
            'birth_facility',

            'birthweight',
            'low_birthweight',
            'gestational_age',
            'preterm_birth',

            'nicu_admission',

            'hepatitis_b_immunization',
            'infant_feeding',

            'total_births'
        ],

        ARRAY[
            maternal_age_range,
            maternal_education_label,
            hispanic_label,
            white_label,
            black_label,
            asian_label,
            pacific_islander_label,
            american_indian_alaskan_native_label,
            other_race_label,
            primary_payor_label,

            tobacco_label,
            alcohol_risk_label,
            drug_use_label,
            diabetes_pre_preg_label,
            gestation_diabetes_label,
            hypertension_pre_preg_label,
            hypertension_preg_related_label,
            maternal_bmi_range,

            prenatal_care_received_label,
            prenatal_care_adequacy,

            delivery_method_label,

            CASE
                WHEN delivery_method_label IS NULL
                    OR delivery_method_label = 'Unknown'
                    THEN 'Unknown'
                WHEN delivery_method_label = 'Cesarean'
                    THEN 'Yes'
                ELSE 'No'
            END,

            facility_label,

            birthweight_range,

            CASE
                WHEN birthweight_grams IS NULL
                    OR birthweight_grams <= 0
                    THEN 'Unknown'
                WHEN birthweight_grams < 2500
                    THEN 'Yes'
                ELSE 'No'
            END,

            gestational_age_range,

            CASE
                WHEN gestational_age_weeks IS NULL
                    OR gestational_age_weeks <= 0
                    THEN 'Unknown'
                WHEN gestational_age_weeks < 37
                    THEN 'Yes'
                ELSE 'No'
            END,

            CASE
                WHEN nicu_admission = '1' THEN 'Yes'
                WHEN nicu_admission = '0' THEN 'No'
                ELSE 'Unknown'
            END,

            hep_b_immunization_label,
            infant_feeding_label,

            'Total'
        ]

    ) AS t(indicator_id, indicator_category)

),

indicator_totals AS (

    SELECT
        geographic_area,
        birth_year,
        geographic_level,
        locality,

        maternal_age_group,
        maternal_ethnicity,
        maternal_race_white,
        maternal_race_black,
        maternal_race_asian,
        maternal_race_pacific_islander,
        maternal_race_aian,
        maternal_race_other,

        indicator_id,

        COUNT(*) AS total_births,

        COUNT_IF(indicator_category <> 'Unknown')
            AS denominator,

        COUNT_IF(indicator_category = 'Unknown')
            AS unknown_count

    FROM standardized_indicators

    GROUP BY
        geographic_area,
        birth_year,
        geographic_level,
        locality,

        maternal_age_group,
        maternal_ethnicity,
        maternal_race_white,
        maternal_race_black,
        maternal_race_asian,
        maternal_race_pacific_islander,
        maternal_race_aian,
        maternal_race_other,

        indicator_id

),

category_counts AS (

    SELECT
        geographic_area,
        birth_year,
        geographic_level,
        locality,

        maternal_age_group,
        maternal_ethnicity,
        maternal_race_white,
        maternal_race_black,
        maternal_race_asian,
        maternal_race_pacific_islander,
        maternal_race_aian,
        maternal_race_other,

        indicator_id,
        indicator_category,

        COUNT(*) AS numerator

    FROM standardized_indicators

    WHERE indicator_category <> 'Unknown'

    GROUP BY
        geographic_area,
        birth_year,
        geographic_level,
        locality,

        maternal_age_group,
        maternal_ethnicity,
        maternal_race_white,
        maternal_race_black,
        maternal_race_asian,
        maternal_race_pacific_islander,
        maternal_race_aian,
        maternal_race_other,

        indicator_id,
        indicator_category

),

indicator_categories AS (

    SELECT DISTINCT
        indicator_id,
        indicator_category

    FROM standardized_indicators

    WHERE indicator_category <> 'Unknown'

    UNION

    SELECT
        indicator_id,
        indicator_category

    FROM UNNEST(

        ARRAY[
            'maternal_race_white',
            'maternal_race_black',
            'maternal_race_asian',
            'maternal_race_pacific_islander',
            'maternal_race_aian',
            'maternal_race_other',
            'cesarean_delivery',
            'low_birthweight',
            'preterm_birth',
            'nicu_admission'
        ]

    ) AS t(indicator_id)

    CROSS JOIN UNNEST(
        ARRAY['Yes', 'No']
    ) AS c(indicator_category)

),

indicator_mapping AS (

    SELECT
        TRIM(indicator_id) AS indicator_id,
        TRIM(indicator_title) AS indicator_title,
        TRIM(indicator_group) AS indicator_group,
        TRY_CAST(sort_order AS INTEGER) AS sort_order

    FROM michc_db_reference.ref_michc_indicator_mapping

)

SELECT

    -- Indicator metadata

    it.indicator_id,
    im.indicator_title,
    im.indicator_group,
    ic.indicator_category,
    im.sort_order,

    -- Geography

    it.geographic_level,
    it.locality,

    -- Maternal demographic dimensions

    it.maternal_age_group,
    it.maternal_ethnicity,
    it.maternal_race_white,
    it.maternal_race_black,
    it.maternal_race_asian,
    it.maternal_race_pacific_islander,
    it.maternal_race_aian,
    it.maternal_race_other,

    -- Statistical measures

    it.total_births,

    COALESCE(cc.numerator, 0) AS numerator,

    it.denominator,
    it.unknown_count,

    CASE
        WHEN it.denominator > 0
        THEN
            100.0 * COALESCE(cc.numerator, 0)
            / it.denominator
        ELSE NULL
    END AS percentage,

    -- Partition columns (must be last)

    it.geographic_area,
    it.birth_year

FROM indicator_totals it

INNER JOIN indicator_categories ic
    ON it.indicator_id = ic.indicator_id

LEFT JOIN category_counts cc

    ON it.geographic_area = cc.geographic_area
    AND it.birth_year = cc.birth_year
    AND it.geographic_level = cc.geographic_level
    AND it.locality = cc.locality

    AND it.maternal_age_group = cc.maternal_age_group
    AND it.maternal_ethnicity = cc.maternal_ethnicity
    AND it.maternal_race_white = cc.maternal_race_white
    AND it.maternal_race_black = cc.maternal_race_black
    AND it.maternal_race_asian = cc.maternal_race_asian

    AND it.maternal_race_pacific_islander =
        cc.maternal_race_pacific_islander

    AND it.maternal_race_aian = cc.maternal_race_aian
    AND it.maternal_race_other = cc.maternal_race_other

    AND it.indicator_id = cc.indicator_id
    AND ic.indicator_category = cc.indicator_category

INNER JOIN indicator_mapping im
    ON it.indicator_id = im.indicator_id;

"""

# endregion


# ============================================================
# QUICKSIGHT DATASET REFRESH
# ============================================================

def refresh_quicksight_dataset():
    """
    Initiate a full refresh of the MICHC QuickSight
    SPICE dataset.
    """

    ingestion_id = f"refresh-{uuid.uuid4()}"

    print(f"Starting QuickSight ingestion: {ingestion_id}")

    return quicksight.create_ingestion(
        AwsAccountId=ACCOUNT_ID,
        DataSetId=QUICKSIGHT_DATASET_ID,
        IngestionId=ingestion_id,
        IngestionType="FULL_REFRESH"
    )


# ============================================================
# LAMBDA HANDLER
# ============================================================

def lambda_handler(event, context):

    print("Starting MICHC Birth Data Pipeline refresh")

    # --------------------------------------------------------
    # Step 1: Repair raw table partitions
    # --------------------------------------------------------

    print("Repairing raw MICHC table partitions")

    run_athena_query(
        "MSCK REPAIR TABLE michc_db_raw.raw_michc_opencsv",
        database="michc_db_raw"
    )

    print("Raw MICHC table partitions repaired")

    # --------------------------------------------------------
    # Step 2: Drop existing birth records table
    # --------------------------------------------------------

    print("Dropping existing michc_birth_records table")

    run_athena_query(
        "DROP TABLE IF EXISTS michc_db_curated.michc_birth_records"
    )

    # --------------------------------------------------------
    # Step 3: Delete existing birth records from S3
    # --------------------------------------------------------

    print("Deleting existing curated birth records")

    delete_s3_prefix(BIRTH_RECORDS_PREFIX)

    # --------------------------------------------------------
    # Step 4: Rebuild birth records table
    # --------------------------------------------------------

    print("Creating new michc_birth_records table")

    run_athena_query(BIRTH_RECORDS_SQL)

    print("michc_birth_records table created successfully")

    # --------------------------------------------------------
    # Step 5: Drop existing birth indicators table
    # --------------------------------------------------------

    print("Dropping existing michc_birth_indicators table")

    run_athena_query(
        "DROP TABLE IF EXISTS michc_db_curated.michc_birth_indicators"
    )

    # --------------------------------------------------------
    # Step 6: Delete existing birth indicators from S3
    # --------------------------------------------------------

    print("Deleting existing curated birth indicators")

    delete_s3_prefix(BIRTH_INDICATORS_PREFIX)

    # --------------------------------------------------------
    # Step 7: Rebuild birth indicators table
    # --------------------------------------------------------

    print("Creating new michc_birth_indicators table")

    run_athena_query(BIRTH_INDICATORS_SQL)

    print("michc_birth_indicators table created successfully")

    # --------------------------------------------------------
    # Step 8: Refresh QuickSight dataset
    # --------------------------------------------------------

    print("Starting MICHC QuickSight dataset refresh")

    ingestion_response = refresh_quicksight_dataset()

    print("QuickSight dataset refresh initiated")

    # --------------------------------------------------------
    # Step 9: Return pipeline execution status
    # --------------------------------------------------------

    print("MICHC Birth Data Pipeline execution complete")

    return {
        "status": "success",
        "message": (
            "MICHC curated tables rebuilt and "
            "QuickSight refresh initiated successfully."
        ),
        "quicksight_ingestion": {
            "ingestion_id": ingestion_response.get("IngestionId"),
            "ingestion_status": ingestion_response.get(
                "IngestionStatus"
            )
        }
    }


# ============================================================
# END OF MICHC BIRTH DATA PIPELINE
# ============================================================
