with source as (
    select * from {{ ref('stg_stackoverflow_all_years') }}
),

normalized_age as (
    select
        case
            when age is null then null
            when age = 'Prefer not to disclose' then 'Prefer not to disclose'
            when age in ('<18', '< 20', '18-24', '20-24') then '18-24'
            when age in ('25-29', '25-34', '30-34') then '25-34'
            when age in ('35-39', '35-44') then '35-44'
            when age in ('40-49', '40-50') then '45-54'
            when age = '45-54' then '45-54'
            when age in ('50-59', '51-60') then '55-64'
            when age = '55-64' then '55-64'
            when age in ('> 60', '>60', '>65') then '65+'
            else age
        end as age,
    from source
)

select * from normalized_age
