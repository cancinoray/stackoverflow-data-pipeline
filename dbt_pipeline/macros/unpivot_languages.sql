{% macro unpivot_languages(ref_table, column_language_mapping) %}
  {% set query_parts = [] %}

  {% for mapping in column_language_mapping %}
    {% set col_name = mapping[0] %}
    {% set lang_name = mapping[1] %}

    {% set sql_piece %}
      SELECT
        year,
        respondent_id,
        '{{ lang_name }}' AS language,
        CAST({{ col_name }} AS INT64) AS col_resp
      FROM {{ ref(ref_table) }}
    {% endset %}

    {% do query_parts.append(sql_piece) %}
  {% endfor %}

  {{ return(query_parts | join(' UNION ALL ')) }}
{% endmacro %}
