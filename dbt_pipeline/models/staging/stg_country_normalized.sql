-- models/staging/stg_country_normalized.sql
with source as (
    select * from {{ ref('stg_stackoverflow_all_years') }}
),

renamed as (
    select
        case
            when country in ('United States of America', 'USA') then 'United States'
            when country in ('United Kingdom of Great Britain and Northern Ireland') then 'United Kingdom'
            when country in ('Russia', 'Russian Federation') then 'Russia'
            when country in ('Iran, Islamic Republic of...') then 'Iran'
            when country in ('Viet Nam', 'Vietnam') then 'Vietnam'
            when country in ('Korea South', 'South Korea', 'Republic of Korea') then 'South Korea'
            when country in ('Slovak Republic', 'Slovakia') then 'Slovakia'
            when country in ('Bosnia Herzegovina', 'Bosnia and Herzegovina') then 'Bosnia and Herzegovina'
            when country in ('Hong Kong (S.A.R.)', 'Hong Kong') then 'Hong Kong'
            when country = 'NA' or country = 'N/A' then null
            else country
        end as country
    from source
)

select * from renamed