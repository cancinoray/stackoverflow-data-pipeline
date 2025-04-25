{% macro generate_union_surveys() %}
    {% set years = range(2011, 2025) %}

    {% for year in years %}
        SELECT
        -- Create a unique ID for each respondent
            MD5(CONCAT(
                CAST(year AS STRING),
                CAST(ROW_NUMBER() OVER() AS STRING),
                '{{ year }}'  -- Survey year as salt
            )) AS respondent_id,
            CAST(year AS STRING) AS year,
            country,
            sex,
            age,
            education,
            occupation,
            experience_years,
            annual_compensation_usd,
            job_satisfaction,
            tech_own,
            os_used,
            prog_language_proficient_in,
            prog_language_desired
        FROM {{ source('stackoverflow', year ~ '_survey') }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
{% endmacro %}
