
-- ============================================================
-- MICHC BIRTH DATA PIPELINE
-- Curated Birth Indicators Table Creation
-- ============================================================
-- Database: michc_db_curated
-- Table: michc_birth_indicators
--
-- Purpose:
-- Transform standardized MICHC birth records into aggregated
-- maternal and infant health indicators for county-level
-- and locality-level public health analysis.
--
-- Includes:
-- 31 maternal and infant health indicators
-- 8 maternal demographic dimensions
-- County and locality geographic breakdowns
-- Numerators, denominators, unknown counts, and percentages
--
-- Output Format: PARQUET
-- Partitioned by: geographic_area, birth_year
-- ============================================================

CREATE TABLE michc_db_curated.michc_birth_indicators

WITH (
    format = 'PARQUET',
    external_location = 's3://publichealth-project-data-lake-152125350281-us-east-2-an/curated/ny-state/michc/birth_indicators/',
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
