with source as (
    select *
    from {{ ref('stg_stackoverflow_all_years') }}
),

-- This transformation normalizes education responses from StackOverflow surveys
-- into standardized categories for consistent analysis across years
normalized_education as (
    select
        respondent_id,  -- Include the respondent_id in the CTE
        case
            when coalesce(education, '') = '' or lower(education) in ('na', 'null') then 'No Answer'
            when REGEXP_CONTAINS(lower(education), r'\bassociate\b|\bassociates\b') then 'Associate Degree'
            when REGEXP_CONTAINS(lower(education), r'\b(b\.?s\.?|bachelor|bachelors)\b') then 'Bachelor\'s Degree'
            when REGEXP_CONTAINS(lower(education), r'\b(m\.?s\.?|master|masters)\b') then 'Master\'s Degree'
            when REGEXP_CONTAINS(lower(education), r'(phd|ph\.?d\.?|doctoral|doctorate|jd|md|professional degree)') then 'Professional Degree'
            when REGEXP_CONTAINS(lower(education), r'(secondary|high school|realschule|gymnasium|primary|elementary)') then 'Secondary School'
            when REGEXP_CONTAINS(lower(education), r'(some college|some university)') then 'Attend University without earning degree'
            when REGEXP_CONTAINS(lower(education), r'(no formal|on[- ]the[- ]job training)') then 'No Formal Training'
            when REGEXP_CONTAINS(lower(education), r'(self[- ]taught)') then 'Self Taught'
            when REGEXP_CONTAINS(lower(education), r'(online class|coursera|codecademy|khan academy|udemy|edx|mooc)') then 'Online Education'
            else 'Others'
        end as education
    from source
)

select *
from normalized_education