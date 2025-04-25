WITH base_data AS (
    SELECT
        respondent_id,
        year,
        experience_years,
        tech_own,
        prog_language_desired,
        prog_language_proficient_in,
    FROM {{ ref('stg_stackoverflow_all_years') }}
)

SELECT
    -- Base columns (non-normalized)
    b.respondent_id,
    b.year,
    b.experience_years,
    b.tech_own,
    b.prog_language_desired,
    b.prog_language_proficient_in,
    
    -- Normalized demographic fields (replace raw columns)
    age.age,
    country.country,
    sex.sex,
    education.education,
    
    -- Normalized job-related fields
    occupation.occupation,
    compensation.annual_compensation_usd,
    job_satisfaction.job_satisfaction,
    
    -- Normalized tech fields
    os.os_used,
    
    -- Language proficiency flags
    lang.prog_language_used_js,
    lang.prog_language_used_html_css,
    lang.prog_language_used_python,
    lang.prog_language_used_sql,
    lang.prog_language_used_typescript,
    lang.prog_language_used_java,
    lang.prog_language_used_bash_shell,
    lang.prog_language_used_csharp,
    lang.prog_language_used_cpp,
    lang.prog_language_used_php,
    lang.prog_language_used_go,
    lang.prog_language_used_rust,
    lang.prog_language_used_kotlin,
    lang.prog_language_used_ruby,
    lang.prog_language_used_swift,
    lang.prog_language_used_r,
    lang.prog_language_used_dart,
    lang.prog_language_used_scala,
    lang.prog_language_used_elixir,
    
    -- Language interest flags
  
    lang_desired.prog_language_interested_js,
    lang_desired.prog_language_interested_html_css,
    lang_desired.prog_language_interested_python,
    lang_desired.prog_language_interested_sql,
    lang_desired.prog_language_interested_typescript,
    lang_desired.prog_language_interested_java,
    lang_desired.prog_language_interested_bash_shell,
    lang_desired.prog_language_interested_csharp,
    lang_desired.prog_language_interested_cpp,
    lang_desired.prog_language_interested_php,
    lang_desired.prog_language_interested_go,
    lang_desired.prog_language_interested_rust,
    lang_desired.prog_language_interested_kotlin,
    lang_desired.prog_language_interested_ruby,
    lang_desired.prog_language_interested_swift,
    lang_desired.prog_language_interested_r,
    lang_desired.prog_language_interested_dart,
    lang_desired.prog_language_interested_scala,
    lang_desired.prog_language_interested_elixir
    
FROM base_data b
LEFT JOIN {{ ref('stg_age_normalized') }} age ON b.respondent_id = age.respondent_id
LEFT JOIN {{ ref('stg_country_normalized') }} country ON b.respondent_id = country.respondent_id
LEFT JOIN {{ ref('stg_sex_normalized') }} sex ON b.respondent_id = sex.respondent_id
LEFT JOIN {{ ref('stg_education_normalized') }} education ON b.respondent_id = education.respondent_id
LEFT JOIN {{ ref('stg_occupation_normalized') }} occupation ON b.respondent_id = occupation.respondent_id
LEFT JOIN {{ ref('stg_annual_compensation_usd_normalized') }} compensation ON b.respondent_id = compensation.respondent_id
LEFT JOIN {{ ref('stg_job_satisfaction_normalized') }} job_satisfaction ON b.respondent_id = job_satisfaction.respondent_id
LEFT JOIN {{ ref('stg_os_used_normalized') }} os ON b.respondent_id = os.respondent_id
LEFT JOIN {{ ref('stg_prog_language_proficient_in_normalized') }} lang ON b.respondent_id = lang.respondent_id
LEFT JOIN {{ ref('stg_prog_language_desired_normalized') }} lang_desired ON b.respondent_id = lang_desired.respondent_id