WITH source AS (
    SELECT *
    FROM {{ ref('stg_stackoverflow_all_years') }}
),
normalized_job_satisfaction AS (
    SELECT 
        respondent_id,  -- Include the respondent_id in the CTE
        CASE 
            -- Null values
            WHEN job_satisfaction IS NULL THEN 'Not specified'
            WHEN job_satisfaction = 'NA' THEN 'Not specified'
            
            -- Very positive responses
            WHEN job_satisfaction IN ('I love my job', 'Love my job', 'So happy it hurts', 'Extremely satisfied', '10', '9') 
                THEN 'Very satisfied'
                
            -- Positive responses
            WHEN job_satisfaction IN ('I enjoy going to work', 'I\'m somewhat satisfied with my job', 
                                    'Slightly satisfied', 'Moderately satisfied', 'Very satisfied', '8', '7') 
                THEN 'Satisfied'
                
            -- Neutral responses
            WHEN job_satisfaction IN ('I\'m neither satisfied nor dissatisfied', 
                                    'I\'m neither satisfied nor dissatisfied with my job',
                                    'Neither satisfied nor dissatisfied', 'It\'s a paycheck', 
                                    'Its a paycheck', 'It pays the bills', '5', '6') 
                THEN 'Neutral'
                
            -- Negative responses
            WHEN job_satisfaction IN ('I\'m somewhat dissatisfied with my job', 'I\'m not happy in my job',
                                    'Slightly dissatisfied', 'Moderately dissatisfied', '3', '4') 
                THEN 'Dissatisfied'
                
            -- Very negative responses
            WHEN job_satisfaction IN ('Hate my job', 'FML', 'I hate my job', 
                                    'Extremely dissatisfied', 'Very dissatisfied', '0', '1', '2') 
                THEN 'Very dissatisfied'
                
            -- Unemployment related
            WHEN job_satisfaction IN ('I don\'t have a job', 'I wish I had a job!') 
                THEN 'No job'
                
            -- Other/Unknown
            WHEN job_satisfaction = 'Other (please specify)' 
                THEN 'Other'
                
            ELSE 'Other'
        END AS job_satisfaction
    FROM source
)

SELECT *
FROM normalized_job_satisfaction