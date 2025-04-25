WITH source AS (
    SELECT *
    FROM {{ ref('stg_stackoverflow_all_years') }}
),
normalized_os_used AS (
    SELECT
        respondent_id,  -- Include the respondent_id in the CTE
        CASE 
            -- Windows OS family
            WHEN os_used IN ('Windows', 'Windows 10', 'Windows 8', 'Windows 7', 'Windows Vista', 'Windows XP') 
                THEN 'Windows'
                
            -- macOS family
            WHEN os_used IN ('Mac OS X', 'Mac OS') 
                THEN 'macOS'
                
            -- Linux distributions
            WHEN os_used IN ('Ubuntu', 'Mint', 'Fedora', 'Debian', 'Linux', 'Other Linux') 
                THEN 'Linux'
                
            -- Null values
            WHEN os_used IS NULL 
                THEN 'Not specified'
                
            -- Other/joke responses
            WHEN os_used IN ('Dogs', 'Cats') 
                THEN 'Invalid response'
                
            -- Other responses
            WHEN os_used IN ('Other', 'Others', 'Other (please specify)') 
                THEN 'Other'
                
            ELSE 'Other'
        END AS os_used
    FROM source
)

SELECT *
FROM normalized_os_used