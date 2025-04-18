from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id
from pyspark.sql import types


def transform_2016(df):
    df_2016_raw = df.select(
        "country",
        "age_range",
        "gender",
        "occupation",
        "experience_range",
        "salary_range",
        "tech_do",
        "tech_want",
        "job_satisfaction",
        "desktop_os",
        "education",
    )

    df_2016_raw = df_2016_raw.select(
        col("country").alias("country"),
        col("age_range").alias("age"),
        col("gender").alias("sex"),
        col("occupation").alias("occupation"),
        col("experience_range").alias("experience_years"),
        col("salary_range").alias("annual_compensation"),
        col("tech_do").alias("prog_language_proficient_in"),
        col("tech_want").alias("prog_language_desired"),
        col("job_satisfaction").alias("job_satisfaction"),
        col("desktop_os").alias("os_used"),
        col("education").alias("education"),
    )

    # adding year column
    df_2016_raw = df_2016_raw.withColumn("year", lit("2016"))

    # adding new columns
    new_columns = ["tech_own"]

    for col_name in new_columns:
        df_2016_raw = df_2016_raw.withColumn(col_name, lit(None))

    # Reorder columns
    reordered_columns = [
        "year",
        "country",
        "sex",
        "age",
        "education",
        "occupation",
        "experience_years",
        "annual_compensation",
        "job_satisfaction",
        "tech_own",
        "os_used",
        "prog_language_proficient_in",
        "prog_language_desired"
    ]

    df_2016 = df_2016_raw.select(*reordered_columns)

    # schema validation and editing
    df_2016 = df_2016.withColumn("year", col("year").cast(types.IntegerType())) \
        .withColumn("tech_own", col("tech_own").cast("string")) \

    df_2016.printSchema()

    df_2016.groupBy("experience_years").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2016 = df_2016.withColumn(
        "experience_years",
        when(col("experience_years") == "11+ years", "11+")
        .when(col("experience_years") == "2 - 5 years", "2-5")
        .when(col("experience_years") == "6 - 10 years", "6-10")
        .when(col("experience_years") == "1 - 2 years", "<2")
        .when(col("experience_years") == "Less than 1 year", "<2")
        .otherwise(None)
    )

    df_2016.groupBy("experience_years").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2016.groupBy("annual_compensation").count().orderBy(
        "count", ascending=False).show(truncate=False)

    # cleaning the annual_compensation column
    # Clean and create the new column
    df_2016 = df_2016.withColumn(
        "annual_compensation_usd",
        when(col("annual_compensation") == "Unemployed", "0")
        .when(col("annual_compensation") == "Rather not say", None)
        .when(col("annual_compensation") == "Less than $10,000", "<10,000")
        .when(col("annual_compensation") == "More than $200,000", ">200,000")
        .otherwise(regexp_replace(col("annual_compensation"), r"\$", ""))
    )

    # Drop the original column
    df_2016 = df_2016.drop("annual_compensation")

    # Check result
    df_2016.groupBy("annual_compensation_usd").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2016.show(5, truncate=False)
    return df_2016
