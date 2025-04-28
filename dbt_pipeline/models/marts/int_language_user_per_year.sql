WITH unpivoted AS (

  {{ unpivot_languages(
    ref_table = 'fct_survey_results',
    column_language_mapping = [
      ('prog_language_used_js', 'JavaScript'),
      ('prog_language_used_html_css', 'HTML/CSS'),
      ('prog_language_used_python', 'Python'),
      ('prog_language_used_sql', 'SQL'),
      ('prog_language_used_typescript', 'TypeScript'),
      ('prog_language_used_java', 'Java'),
      ('prog_language_used_bash_shell', 'Bash/Shell'),
      ('prog_language_used_csharp', 'C#'),
      ('prog_language_used_cpp', 'C++'),
      ('prog_language_used_php', 'PHP'),
      ('prog_language_used_go', 'Go'),
      ('prog_language_used_rust', 'Rust'),
      ('prog_language_used_kotlin', 'Kotlin'),
      ('prog_language_used_ruby', 'Ruby'),
      ('prog_language_used_swift', 'Swift'),
      ('prog_language_used_r', 'R'),
      ('prog_language_used_dart', 'Dart'),
      ('prog_language_used_scala', 'Scala'),
      ('prog_language_used_elixir', 'Elixir')
    ]
  ) }}

)

SELECT
  SAFE_CAST(year AS INT64) AS year,
  language,
  SUM(CASE WHEN col_resp = 1 THEN 1 ELSE 0 END) AS total_user
FROM unpivoted
WHERE year BETWEEN 2011 AND 2024
GROUP BY year, language
ORDER BY year, total_user DESC
