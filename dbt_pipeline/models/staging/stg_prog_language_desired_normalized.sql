WITH source AS (
    SELECT *,
           {{ standardize_languages('prog_language_desired') }} AS languages_standardized
    FROM {{ ref('stg_stackoverflow_all_years') }}
),

language_flags AS (
    SELECT
      respondent_id,  -- Include the respondent_id in the CTE
      {{ detect_language_flag('languages_standardized', 'javascript') }} AS prog_language_interested_js,
      {{ detect_language_flag('languages_standardized', 'html_css') }} AS prog_language_interested_html_css,
      {{ detect_language_flag('languages_standardized', 'python') }} AS prog_language_interested_python,
      {{ detect_language_flag('languages_standardized', 'sql') }} AS prog_language_interested_sql,
      {{ detect_language_flag('languages_standardized', 'typescript') }} AS prog_language_interested_typescript,
      {{ detect_language_flag('languages_standardized', 'java') }} AS prog_language_interested_java,
      {{ detect_language_flag('languages_standardized', 'bash_shell') }} AS prog_language_interested_bash_shell,
      {{ detect_language_flag('languages_standardized', 'csharp') }} AS prog_language_interested_csharp,
      {{ detect_language_flag('languages_standardized', 'cpp') }} AS prog_language_interested_cpp,
      {{ detect_language_flag('languages_standardized', 'php') }} AS prog_language_interested_php,
      {{ detect_language_flag('languages_standardized', 'go') }} AS prog_language_interested_go,
      {{ detect_language_flag('languages_standardized', 'rust') }} AS prog_language_interested_rust,
      {{ detect_language_flag('languages_standardized', 'kotlin') }} AS prog_language_interested_kotlin,
      {{ detect_language_flag('languages_standardized', 'ruby') }} AS prog_language_interested_ruby,
      {{ detect_language_flag('languages_standardized', 'swift') }} AS prog_language_interested_swift,
      {{ detect_language_flag('languages_standardized', 'r') }} AS prog_language_interested_r,
      {{ detect_language_flag('languages_standardized', 'dart') }} AS prog_language_interested_dart,
      {{ detect_language_flag('languages_standardized', 'scala') }} AS prog_language_interested_scala,
      {{ detect_language_flag('languages_standardized', 'elixir') }} AS prog_language_interested_elixir
    FROM source
)

SELECT *
FROM language_flags