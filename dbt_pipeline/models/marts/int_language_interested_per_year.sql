WITH unpivoted AS (

  {{ unpivot_languages(
    ref_table = 'fct_survey_results',
    column_language_mapping = [
      ('prog_language_interested_js', 'JavaScript'),
      ('prog_language_interested_html_css', 'HTML/CSS'),
      ('prog_language_interested_python', 'Python'),
      ('prog_language_interested_sql', 'SQL'),
      ('prog_language_interested_typescript', 'TypeScript'),
      ('prog_language_interested_java', 'Java'),
      ('prog_language_interested_bash_shell', 'Bash/Shell'),
      ('prog_language_interested_csharp', 'C#'),
      ('prog_language_interested_cpp', 'C++'),
      ('prog_language_interested_php', 'PHP'),
      ('prog_language_interested_go', 'Go'),
      ('prog_language_interested_rust', 'Rust'),
      ('prog_language_interested_kotlin', 'Kotlin'),
      ('prog_language_interested_ruby', 'Ruby'),
      ('prog_language_interested_swift', 'Swift'),
      ('prog_language_interested_r', 'R'),
      ('prog_language_interested_dart', 'Dart'),
      ('prog_language_interested_scala', 'Scala'),
      ('prog_language_interested_elixir', 'Elixir')
    ]
  ) }}

)

SELECT
  SAFE_CAST(year AS INT64) AS year,
  language,
  SUM(CASE WHEN col_resp = 1 THEN 1 ELSE 0 END) AS total_interested
FROM unpivoted
WHERE year BETWEEN 2011 AND 2024
GROUP BY year, language
ORDER BY year, total_interested DESC
