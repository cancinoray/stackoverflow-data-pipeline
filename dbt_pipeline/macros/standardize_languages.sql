{# macros/standardize_languages.sql #}
{% macro standardize_languages(language_field) %}
  {# Standardizes the language string format #}
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        LOWER(TRIM({{ language_field }})),
        r'[,/]', ';'
      ),
      r'[;]+', ';'
    ),
    r'\s+', ' '
  )
{% endmacro %}