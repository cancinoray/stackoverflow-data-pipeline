{# macros/detect_language_flag.sql #}
{% macro detect_language_flag(language_string, language_name) %}
  CASE
    WHEN {{ language_string }} IS NULL OR {{ language_string }} = '' THEN 0
    {% if language_name == 'javascript' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)(javascript|js|node\.js|nodejs|es6|ecmascript)(;|$)') THEN 1
    {% elif language_name == 'html_css' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)(html|css|html5|css3|sass|scss|less)(;|$)') THEN 1
    {% elif language_name == 'python' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)python(2|3)?(;|$)') THEN 1
    {% elif language_name == 'sql' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)(sql|mysql|postgresql|sqlite|mariadb|tsql|plsql)(;|$)') THEN 1
    {% elif language_name == 'typescript' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)(typescript|ts)(;|$)') THEN 1
    {% elif language_name == 'java' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)java(;|$)') 
           AND NOT REGEXP_CONTAINS({{ language_string }}, r'(^|;)javascript(;|$)') THEN 1
    {% elif language_name == 'bash_shell' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)(bash|shell|sh|zsh|powershell|ps1)(;|$)') THEN 1
    {% elif language_name == 'csharp' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)(c#|cs|csharp|\.net)(;|$)') THEN 1
    {% elif language_name == 'cpp' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)(c\+\+|cpp|clang)(;|$)') THEN 1
    {% elif language_name == 'php' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)php(;|$)') THEN 1
    {% elif language_name == 'go' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)(go|golang)(;|$)') THEN 1
    {% elif language_name == 'rust' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)rust(;|$)') THEN 1
    {% elif language_name == 'kotlin' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)kotlin(;|$)') THEN 1
    {% elif language_name == 'ruby' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)ruby(;|$)') THEN 1
    {% elif language_name == 'swift' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)swift(;|$)') THEN 1
    {% elif language_name == 'r' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)r(;|$)') 
           AND NOT REGEXP_CONTAINS({{ language_string }}, r'(^|;)(react|redux)(;|$)') THEN 1
    {% elif language_name == 'dart' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)dart(;|$)') THEN 1
    {% elif language_name == 'scala' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)scala(;|$)') THEN 1
    {% elif language_name == 'elixir' %}
      WHEN REGEXP_CONTAINS({{ language_string }}, r'(^|;)elixir(;|$)') THEN 1
    {% else %}
      0
    {% endif %}
  END
{% endmacro %}