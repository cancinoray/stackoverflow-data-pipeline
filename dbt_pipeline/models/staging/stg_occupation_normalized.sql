WITH source AS (
    SELECT *
    FROM {{ ref('stg_stackoverflow_all_years') }}
),
normalized_occupation AS (
    SELECT
      CASE
          -- Development Roles
          WHEN occupation LIKE '%Developer, back-end%' OR occupation LIKE '%Back-end%' THEN 'Backend Development'
          WHEN occupation LIKE '%Developer, front-end%' OR occupation LIKE '%Front-end%' THEN 'Frontend Development'
          WHEN occupation LIKE '%Developer, full-stack%' OR occupation LIKE '%Web%' OR occupation LIKE '%Developer experience%' OR occupation LIKE '%Full-stack%' THEN 'Full Stack Development'
          WHEN occupation LIKE '%Developer, mobile%' THEN 'Mobile Development'
          WHEN occupation LIKE '%Developer, embedded%' THEN 'Embedded Development'
          WHEN occupation LIKE '%Developer, desktop%' OR occupation LIKE '%enterprise applications%' THEN 'Desktop/Enterprise Development'
          WHEN occupation LIKE '%Developer, game%' OR occupation LIKE '%graphics%' THEN 'Game/Graphics Development'
          WHEN occupation LIKE '%Developer, QA%' OR occupation LIKE '%test%' THEN 'QA/Testing'
          
          -- Data Roles
          WHEN occupation LIKE '%Data scientist%' OR occupation LIKE '%machine learning%' OR occupation LIKE '%Machine learning%' OR occupation LIKE '%AI%' THEN 'Data Science/ML/AI'
          WHEN occupation LIKE '%Engineer, data%' THEN 'Data Engineering'
          WHEN occupation LIKE '%Data or business analyst%' OR occupation LIKE '%business analyst%' THEN 'Data/Business Analysis'
          WHEN occupation LIKE '%Database administrator%' THEN 'Database Administration'
          
          -- Operations Roles
          WHEN occupation LIKE '%DevOps%' THEN 'DevOps'
          WHEN occupation LIKE '%Engineer, site reliability%' THEN 'Site Reliability Engineering'
          WHEN occupation LIKE '%System administrator%' THEN 'System Administration'
          
          -- Leadership & Management
          WHEN occupation LIKE '%Engineering manager%' THEN 'Engineering Management'
          WHEN occupation LIKE '%Product manager%' THEN 'Product Management'
          WHEN occupation LIKE '%Senior executive%' OR occupation LIKE '%VP%' THEN 'Executive Leadership'
          
          -- Other Categories
          WHEN occupation LIKE '%Designer%' THEN 'Design'
          WHEN occupation LIKE '%Educator%' THEN 'Education'
          WHEN occupation LIKE '%Academic researcher%' OR occupation LIKE '%Scientist%' THEN 'Research/Science'
          WHEN occupation LIKE '%Student%' THEN 'Student'
          WHEN occupation LIKE '%Marketing or sales%' THEN 'Marketing/Sales'

          WHEN occupation LIKE '%NA%' THEN 'Self-employed'
          WHEN occupation IS NULL THEN 'Prefered not to disclose'
          

          ELSE 'Other'
        END AS occupation
    FROM source
)


SELECT *
FROM normalized_occupation