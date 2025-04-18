from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id
from pyspark.sql import types


def transform_2013(df):
    # selecting columns to be used
    df_2013_raw = df.select(
        "What Country or Region do you live in?",
        "How old are you?",
        "How many years of IT/Programming experience do you have?",
        "Which of the following best describes your occupation?",
        "Which of the following languages or technologies have you used significantly in the past year?",
        "_c57",
        "_c58",
        "_c59",
        "_c60",
        "_c61",
        "_c62",
        "_c63",
        "_c64",
        "_c65",
        "_c66",
        "_c67",
        "_c68",
        "_c69",
        "Which technologies are you excited about?",
        "_c71",
        "_c72",
        "_c73",
        "_c74",
        "_c75",
        "_c76",
        "_c77",
        "_c78",
        "_c79",
        "_c80",
        "Have you changed jobs in the last 12 months?",
        "Which desktop operating system do you use the most?",
        "What best describes your career / job satisfaction?",
        "Including bonus, what is your annual compensation in USD?",
        "Which technology products do you own? (You can choose more than one)",
        "_c103",
        "_c104",
        "_c105",
        "_c106",
        "_c107",
        "_c108",
        "_c109",
        "_c110",
        "_c111",
        "_c112",
        "_c113",
        "_c114",
    )

    # Concatenating the columns for programming languages
    # List all the relevant columns
    columns = [
        "Which of the following languages or technologies have you used significantly in the past year?",
        "_c57",
        "_c58",
        "_c59",
        "_c60",
        "_c61",
        "_c62",
        "_c63",
        "_c64",
        "_c65",
        "_c66",
        "_c67",
        "_c68",
        "_c69"
    ]

    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]

    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2013_raw = df_2013_raw.withColumn(
        "prog_language_proficient_in", concat_ws(", ", *cleaned_cols))

    # Drop the original columns
    df_2013_raw = df_2013_raw.drop(*columns)

    # Concatenating the columns for technology own
    # List all the relevant columns
    columns = [
        "Including bonus, what is your annual compensation in USD?",
        "Which technology products do you own? (You can choose more than one)",
        "_c103",
        "_c104",
        "_c105",
        "_c106",
        "_c107",
        "_c108",
        "_c109",
        "_c110",
        "_c111",
        "_c112",
        "_c113",
        "_c114",
    ]

    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]

    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2013_raw = df_2013_raw.withColumn(
        "tech_own", concat_ws(", ", *cleaned_cols))

    # Drop the original columns
    df_2013_raw = df_2013_raw.drop(*columns)

    # Concatenating the columns for technology own
    # List all the relevant columns
    columns = [
        "Which technologies are you excited about?",
        "_c71",
        "_c72",
        "_c73",
        "_c74",
        "_c75",
        "_c76",
        "_c77",
        "_c78",
        "_c79",
        "_c80",
    ]

    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]

    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2013_raw = df_2013_raw.withColumn(
        "prog_language_desired", concat_ws(", ", *cleaned_cols))

    # Drop the original columns
    df_2013_raw = df_2013_raw.drop(*columns)

    df_2013_raw = df_2013_raw.select(
        col("What Country or Region do you live in?").alias("country"),
        col("How old are you?").alias("age"),
        col("How many years of IT/Programming experience do you have?").alias("experience_years"),
        col("Which of the following best describes your occupation?").alias(
            "occupation"),
        col("Which desktop operating system do you use the most?").alias("os_used"),
        col("Have you changed jobs in the last 12 months?").alias(
            "job_satisfaction"),
        col("What best describes your career / job satisfaction?").alias("annual_compensation"),
        col("prog_language_proficient_in").alias(
            "prog_language_proficient_in"),
        col("tech_own").alias("tech_own"),
        col("prog_language_desired").alias("prog_language_desired")
    )

    # Removing the first row
    # Add index
    df_2013_raw = df_2013_raw.withColumn(
        'index', monotonically_increasing_id())

    # remove rows by filtering
    df_2013_raw = df_2013_raw.filter(~df_2013_raw.index.isin(0))

    # drop the index column
    df_2013_raw = df_2013_raw.drop("index")

    # adding year column
    df_2013_raw = df_2013_raw.withColumn("year", lit("2013"))

    # adding new columns
    new_columns = ["sex", "education"]

    for col_name in new_columns:
        df_2013_raw = df_2013_raw.withColumn(col_name, lit(None))

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

    df_2013 = df_2013_raw.select(*reordered_columns)

    # schema validation and editing
    df_2013 = df_2013.withColumn("year", col("year").cast(types.IntegerType())) \
        .withColumn("sex", col("sex").cast("string")) \
        .withColumn("education", col("education").cast("string")) \
        .withColumn("prog_language_desired", col("prog_language_desired").cast("string"))

    df_2013.groupBy("experience_years").count().orderBy(
        "count", ascending=False).show(truncate=False)

    # cleaning experience_years column
    df_2013 = df_2013.withColumn(
        "experience_years",
        when(col("experience_years") == "11", "11+")
        .when(col("experience_years") == "2/5/2013", "2-5")
        .when(col("experience_years") == "6/10/2013", "6-10")
        .when(col("experience_years") == "<2", "<2")
        .otherwise(None)
    )

    df_2013.groupBy("experience_years").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2013.groupBy("annual_compensation").count().orderBy(
        "count", ascending=False).show(truncate=False)

    # cleaning the annual_compensation column
    # Clean and create the new column
    df_2013 = df_2013.withColumn(
        "annual_compensation_usd",
        when(col("annual_compensation") == "Student / Unemployed", "0")
        .when(col("annual_compensation") == "Rather not say", None)
        .otherwise(regexp_replace(col("annual_compensation"), r"\$", ""))
    )

    # Drop the original column
    df_2013 = df_2013.drop("annual_compensation")

    # Check result
    df_2013.groupBy("annual_compensation_usd").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2013.groupBy("job_satisfaction").count().orderBy(
        "count", ascending=False).show(truncate=False)

    df_2013.show()
    return df_2013
