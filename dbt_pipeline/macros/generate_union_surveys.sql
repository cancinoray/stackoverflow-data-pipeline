{% macro generate_union_surveys() %}
    {% set years = range(2011, 2025) %}

    {% for year in years %}
        SELECT
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
