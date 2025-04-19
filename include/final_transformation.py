from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, trim, regexp_replace, concat_ws, monotonically_increasing_id, row_number, lower, udf
from pyspark.sql import types
import re
from functools import reduce
import os


def clean_and_transform_survey_data():
    project_id = os.getenv("GCP_PROJECT", "stackoverflow-survey-456106")
    dataset = os.getenv("GCP_BGQUERY", "stackoverflow_survey_dataset")
    bucket_name = os.getenv(
        "GCS_BUCKET", "gsbucket-stackoverflow-survey-456106")
    bq_table = "stackoverflow_survey_2011_2024_combined"

    # Start SparkSession with BigQuery support
    spark = SparkSession.builder \
        .appName("CleanTransformSurveyData") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1") \
        .getOrCreate()

    # Read from BigQuery
    df = spark.read.format("bigquery") \
        .option("project", project_id) \
        .option("dataset", dataset) \
        .option("table", bq_table) \
        .load()

    # Perform transformations

    # === country ===#
    df.filter(col("country").isNull()).count()  # Check nulls
    df.filter(col("country") == "NA").count()   # Check "NA" strings

    # Replace them:
    df = df.replace("NA", None)

    # === age ===#
    df = df.withColumn(
        "age",
        when(col("age").isin("<18", "< 20"), "<18")
        .when(col("age").isin("18-24", "20-24"), "18-24")
        .when(col("age").isin("25-34", "25-29", "30-34"), "25-34")
        .when(col("age").isin("35-44", "35-39", "40-44"), "35-44")
        .when(col("age").isin("45-54", "40-50", "40-49", "45-49", "50-54"), "45-54")
        .when(col("age").isin("55-64"), "55-64")
        .when(col("age").isin(">60", "> 60", ">65", "60+", "51-60", "50-59"), "65+")
        .when(col("age").isNull(), "Unknown")
        .when(lower(col("age")) == "prefer not to disclose", "Unknown")
        .otherwise("Unknown")
    )

    # === sex ===#
    df = df.withColumn(
        "sex",
        when(col("sex").isNull(), "Unknown")
        .when(lower(col("sex")).isin("prefer not to disclose", "i prefer not to answer", "i don't know/not sure"), "Unknown")
        .when(lower(col("sex")).rlike("degree|high schoocollege|primary|no education"), "Unknown")
        .when(lower(col("sex")) == "male", "Male")
        .when(lower(col("sex")) == "female", "Female")
        .when(lower(col("sex")).contains("non-binary"), "Non-binary")
        .when(lower(col("sex")).contains("genderqueer"), "Non-binary")
        .when(lower(col("sex")).contains("gender non-conforming"), "Non-binary")
        .when(lower(col("sex")).contains("transgender"), "Transgender")
        .when(lower(col("sex")).contains("other"), "Other")
        .when(col("sex").contains(";"), "Multiple")
        .otherwise("Other")
    )

    # === job_satisfaction ===#
    df = df.withColumn(
        "job_satisfaction",
        when(col("job_satisfaction").isNull(), "Unknown")

        # Very satisfied
        .when(lower(col("job_satisfaction")).isin("very satisfied", "extremely satisfied", "i love my job", "love my job", "so happy it hurts"), "Very satisfied")

        # Moderately satisfied
        .when(lower(col("job_satisfaction")).isin("moderately satisfied", "i'm somewhat satisfied with my job", "i enjoy going to work", "i enjoy going to work", "7", "8", "9", "10"), "Moderately satisfied")

        # Slightly satisfied
        .when(lower(col("job_satisfaction")).isin("slightly satisfied", "6"), "Slightly satisfied")

        # Neutral
        .when(lower(col("job_satisfaction")).rlike("neither satisfied nor dissatisfied"), "Neutral")
        .when(lower(col("job_satisfaction")).isin("5", "it's a paycheck", "its a paycheck", "it pays the bills", "i don't have a job", "i wish i had a job!"), "Neutral")

        # Slightly dissatisfied
        .when(lower(col("job_satisfaction")).isin("slightly dissatisfied"), "Slightly dissatisfied")

        # Moderately dissatisfied
        .when(lower(col("job_satisfaction")).isin("moderately dissatisfied", "i'm somewhat dissatisfied with my job"), "Moderately dissatisfied")

        # Very dissatisfied
        .when(lower(col("job_satisfaction")).isin("very dissatisfied", "extremely dissatisfied", "i'm not happy in my job", "i hate my job", "hate my job", "fml"), "Very dissatisfied")

        # Hate my job
        .when(lower(col("job_satisfaction")).isin("0", "1", "2", "3", "4"), "Very dissatisfied")

        # Salary values
        .when(col("job_satisfaction").rlike("^[0-9]{1,3},?[0-9]{0,3} - [0-9]{1,3},?[0-9]{0,3}$"), "Other")
        .when(col("job_satisfaction").rlike("^<|>"), "Other")

        # Catch-all
        .otherwise("Other")
    )

    # === annual_compensation_usd ===#
    df = df.withColumn(
        "annual_compensation_usd",
        when(col("annual_compensation_usd").isNull(), "Unknown")

        # Ranges
        .when(col("annual_compensation_usd").rlike("(?i)^<10,?000$"), "<10k")
        .when(col("annual_compensation_usd").rlike("(?i)^10,?000 - 20,?000$"), "10k–20k")
        .when(col("annual_compensation_usd").rlike("(?i)^20,?000 - 30,?000$"), "20k–30k")
        .when(col("annual_compensation_usd").rlike("(?i)^30,?000 - 40,?000$"), "30k–40k")
        .when(col("annual_compensation_usd").rlike("(?i)^40,?000 - 50,?000$"), "40k–50k")
        .when(col("annual_compensation_usd").rlike("(?i)^50,?000 - 60,?000$"), "50k–60k")
        .when(col("annual_compensation_usd").rlike("(?i)^60,?000 - 70,?000$"), "60k–70k")
        .when(col("annual_compensation_usd").rlike("(?i)^70,?000 - 80,?000$"), "70k–80k")
        .when(col("annual_compensation_usd").rlike("(?i)^80,?000 - 90,?000$"), "80k–90k")
        .when(col("annual_compensation_usd").rlike("(?i)^90,?000 - 100,?000$"), "90k–100k")
        .when(col("annual_compensation_usd").rlike("(?i)^100,?000 - 110,?000$"), "100k–110k")
        .when(col("annual_compensation_usd").rlike("(?i)^110,?000 - 120,?000$"), "110k–120k")
        .when(col("annual_compensation_usd").rlike("(?i)^120,?000 - 130,?000$"), "120k–130k")
        .when(col("annual_compensation_usd").rlike("(?i)^130,?000 - 140,?000$"), "130k–140k")
        .when(col("annual_compensation_usd").rlike("(?i)^140,?000 - 150,?000$"), "140k–150k")
        .when(col("annual_compensation_usd").rlike("(?i)^150,?000 - 160,?000$"), "150k–160k")
        .when(col("annual_compensation_usd").rlike("(?i)^160,?000 - 170,?000$"), "160k–170k")
        .when(col("annual_compensation_usd").rlike("(?i)^>200,?000$"), ">200k")
        .when(col("annual_compensation_usd").rlike("(?i)^<20,?000$"), "<20k")
        .when(col("annual_compensation_usd").rlike("(?i)^0$"), "0")
        .when(col("annual_compensation_usd").rlike("^>160,?000$"), ">160k")
        .when(col("annual_compensation_usd").rlike("^>140,?000$"), ">140k")

        # Grouped Ranges
        .when(col("annual_compensation_usd").rlike("^20,?000 - 40,?000$"), "20k–40k")
        .when(col("annual_compensation_usd").rlike("^40,?000 - 60,?000$"), "40k–60k")
        .when(col("annual_compensation_usd").rlike("^60,?000 - 80,?000$"), "60k–80k")
        .when(col("annual_compensation_usd").rlike("^80,?000 - 100,?000$"), "80k–100k")
        .when(col("annual_compensation_usd").rlike("^100,?000 - 120,?000$"), "100k–120k")
        .when(col("annual_compensation_usd").rlike("^120,?000 - 140,?000$"), "120k–140k")
        .when(col("annual_compensation_usd").rlike("^140,?000 - 160,?000$"), "140k–160k")

        # Everything else (languages, techs, weird stuff)
        .otherwise("Other")
    )

    df = df.drop("survey_year")

    # Write back to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", f"{project_id}.{dataset}.cleaned-stackoverflow-combined") \
        .option("temporaryGcsBucket", bucket_name) \
        .option("writeMethod", "direct") \
        .mode("overwrite") \
        .save()

    spark.stop()
