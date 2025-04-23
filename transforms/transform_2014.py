from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, regexp_replace, concat_ws, monotonically_increasing_id
from pyspark.sql import types


def transform_2014(df):
    df_2014_raw = df.select(
        "What Country do you live in?",
        "How old are you?",
        "What is your gender?",
        "How many years of IT/Programming experience do you have?",
        "Which of the following best describes your occupation?",
        "Including bonus, what is your annual compensation in USD?",
        "Which of the following languages or technologies have you used significantly in the past year?",
        "_c43",
        "_c44",
        "_c45",
        "_c46",
        "_c47",
        "_c48",
        "_c49",
        "_c50",
        "_c51",
        "_c52",
        "_c53",
        "Which technologies are you excited about?",
        "_c55",
        "_c56",
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
        "Which desktop operating system do you use the most?",
        "Which technology products do you own? (You can choose more than one)",
        "_c69",
        "_c70",
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
        "_c81",
    )

    # Concatenating the columns for programming languages
    # List all the relevant columns
    columns = [
        "Which of the following languages or technologies have you used significantly in the past year?",
        "_c43",
        "_c44",
        "_c45",
        "_c46",
        "_c47",
        "_c48",
        "_c49",
        "_c50",
        "_c51",
        "_c52",
        "_c53"
    ]

    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]
    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2014_raw = df_2014_raw.withColumn(
        "prog_language_proficient_in", concat_ws(", ", *cleaned_cols))
    # Drop the original columns
    df_2014_raw = df_2014_raw.drop(*columns)

    # Concatenating the columns for technology own
    # List all the relevant columns
    columns = [
        "Which technology products do you own? (You can choose more than one)",
        "_c69",
        "_c70",
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
        "_c81",
    ]
    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]
    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2014_raw = df_2014_raw.withColumn(
        "tech_own", concat_ws(", ", *cleaned_cols))
    # Drop the original columns
    df_2014_raw = df_2014_raw.drop(*columns)

    # Concatenating the columns for technology own
    # List all the relevant columns
    columns = [
        "Which technologies are you excited about?",
        "_c55",
        "_c56",
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
    ]
    # Create a cleaned version of each column (null or empty string gets filtered)
    cleaned_cols = [when((col(c).isNotNull()) & (
        col(c) != ""), col(c)) for c in columns]
    # Use concat_ws to join with commas, skipping nulls/empty values
    df_2014_raw = df_2014_raw.withColumn(
        "prog_language_desired", concat_ws(", ", *cleaned_cols))
    # Drop the original columns
    df_2014_raw = df_2014_raw.drop(*columns)

    # Selecting the relevant columns
    df_2014_raw = df_2014_raw.select(
        col("What Country do you live in?").alias("country"),
        col("How old are you?").alias("age"),
        col("What is your gender?").alias("sex"),
        col("How many years of IT/Programming experience do you have?").alias("experience_years"),
        col("Which of the following best describes your occupation?").alias(
            "occupation"),
        col("Which desktop operating system do you use the most?").alias("os_used"),
        col("Including bonus, what is your annual compensation in USD?").alias(
            "annual_compensation"),
        col("prog_language_proficient_in").alias(
            "prog_language_proficient_in"),
        col("tech_own").alias("tech_own"),
        col("prog_language_desired").alias("prog_language_desired")
    )

    # Removing the first row
    # Add index
    df_2014_raw = df_2014_raw.withColumn(
        'index', monotonically_increasing_id())
    # remove rows by filtering
    df_2014_raw = df_2014_raw.filter(~df_2014_raw.index.isin(0))
    # drop the index column
    df_2014_raw = df_2014_raw.drop("index")

    # adding year column
    df_2014_raw = df_2014_raw.withColumn("year", lit("2014"))
    # adding new columns
    new_columns = ["job_satisfaction", "education"]
    for col_name in new_columns:
        df_2014_raw = df_2014_raw.withColumn(col_name, lit(None))

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
    df_2014 = df_2014_raw.select(*reordered_columns)

    # schema validation and editing
    df_2014 = df_2014.withColumn("year", col("year").cast(types.IntegerType())) \
        .withColumn("education", col("education").cast("string")) \
        .withColumn("job_satisfaction", col("job_satisfaction").cast("string"))

    # cleaning experience_years column
    df_2014 = df_2014.withColumn(
        "experience_years",
        when(col("experience_years") == "11", "11+")
        .when(col("experience_years") == "2/5/2014", "2-5")
        .when(col("experience_years") == "6/10/2014", "6-10")
        .when(col("experience_years") == "<2", "<2")
        .otherwise(None)
    )

    # Clean and create the new column
    df_2014 = df_2014.withColumn(
        "annual_compensation_usd",
        when(col("annual_compensation") == "Student / Unemployed", "0")
        .when(col("annual_compensation") == "Rather not say", None)
        .otherwise(regexp_replace(col("annual_compensation"), r"\$", ""))
    )
    # Drop the original column
    df_2014 = df_2014.drop("annual_compensation")

    df_2014.show()
    return df_2014
