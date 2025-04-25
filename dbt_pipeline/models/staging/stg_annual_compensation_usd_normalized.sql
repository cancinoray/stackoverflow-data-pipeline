WITH source AS (
    SELECT *
    FROM {{ ref('stg_stackoverflow_all_years') }}
),
normalized_annual_compensation_usd AS (
    SELECT
        respondent_id,  -- Include the respondent_id in the CTE
        CASE 
            WHEN annual_compensation_usd IS NULL THEN 'Unknown'
            WHEN annual_compensation_usd = 'null' THEN 'Unknown'
            WHEN annual_compensation_usd = '0' THEN '0-20,000'
            WHEN annual_compensation_usd = '<10,000' THEN '<10,000'
            WHEN annual_compensation_usd = '<20,000' THEN '0-20,000'
            WHEN annual_compensation_usd = '10,000 - 20,000' THEN '0-20,000'
            WHEN annual_compensation_usd = '20,000 - 30,000' THEN '20,000-40,000'
            WHEN annual_compensation_usd = '20,000 - 40,000' THEN '20,000-40,000'
            WHEN annual_compensation_usd = '30,000 - 40,000' THEN '20,000-40,000'
            WHEN annual_compensation_usd = '40,000 - 50,000' THEN '40,000-60,000'
            WHEN annual_compensation_usd = '40,000 - 60,000' THEN '40,000-60,000'
            WHEN annual_compensation_usd = '50,000 - 60,000' THEN '40,000-60,000'
            WHEN annual_compensation_usd = '60,000 - 70,000' THEN '60,000-80,000'
            WHEN annual_compensation_usd = '60,000 - 80,000' THEN '60,000-80,000'
            WHEN annual_compensation_usd = '70,000 - 80,000' THEN '60,000-80,000'
            WHEN annual_compensation_usd = '80,000 - 90,000' THEN '80,000-100,000'
            WHEN annual_compensation_usd = '80,000 - 100,000' THEN '80,000-100,000'
            WHEN annual_compensation_usd = '90,000 - 100,000' THEN '80,000-100,000'
            WHEN annual_compensation_usd = '100,000 - 110,000' THEN '100,000-120,000'
            WHEN annual_compensation_usd = '100,000 - 120,000' THEN '100,000-120,000'
            WHEN annual_compensation_usd = '110,000 - 120,000' THEN '100,000-120,000'
            WHEN annual_compensation_usd = '120,000 - 130,000' THEN '120,000-140,000'
            WHEN annual_compensation_usd = '120,000 - 140,000' THEN '120,000-140,000'
            WHEN annual_compensation_usd = '130,000 - 140,000' THEN '120,000-140,000'
            WHEN annual_compensation_usd = '140,000 - 150,000' THEN '140,000-160,000'
            WHEN annual_compensation_usd = '140,000 - 160,000' THEN '140,000-160,000'
            WHEN annual_compensation_usd = '>140,000' THEN '140,000+'
            WHEN annual_compensation_usd = '150,000 - 160,000' THEN '140,000-160,000'
            WHEN annual_compensation_usd = '160,000 - 170,000' THEN '160,000-180,000'
            WHEN annual_compensation_usd = '>160,000' THEN '160,000+'
            WHEN annual_compensation_usd = '170,000 - 180,000' THEN '160,000-180,000'
            WHEN annual_compensation_usd = '180,000 - 190,000' THEN '180,000-200,000'
            WHEN annual_compensation_usd = '190,000 - 200,000' THEN '180,000-200,000'
            WHEN annual_compensation_usd = '>200,000' THEN '200,000+'
            WHEN annual_compensation_usd = 'Other (please specify)' THEN 'Unknown'
            ELSE 'Preferred not to disclose'
        END AS annual_compensation_usd
    FROM source
)

SELECT *
FROM normalized_annual_compensation_usd