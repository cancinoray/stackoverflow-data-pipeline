with source as (
    select * from {{ ref('stg_stackoverflow_all_years') }}
),

normalized_sex as (
    select
        case
            when sex is null or lower(sex) = 'unknown' then 'Unknown'
            when sex in ('Male', 'Female') then sex
            when sex = 'Prefer not to disclose' then 'Prefer not to disclose'
            when sex in ('Transgender', 'Non-binary', 'Multiple', 'Other') then 'Other'
            else 'Unknown'  -- catch-all fallback
        end as sex,
    from source
)

select * from normalized_sex
